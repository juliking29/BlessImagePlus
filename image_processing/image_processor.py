# Lógica central de procesamiento de imágenes 
#!/usr/bin/env python3
"""
Procesador de imágenes distribuido
"""

import os
import time
import json
import random
import logging
import threading
import zipfile
import uuid
from typing import Dict, List, Any, Tuple, Optional
from werkzeug.utils import secure_filename
import grpc

# Importar módulos generados por gRPC
from . import servicioimagen_pb2
from . import servicioimagen_pb2_grpc



from config import (
    DIRECTORIO_SUBIDAS, EXTENSIONES_PERMITIDAS, TRANSFORMACIONES_SOPORTADAS,
    TIMEOUT_GRPC, INTERVALO_LIMPIEZA, DIRECTORIO_ZIP, DIRECTORIO_RESULTADOS_NODOS
)
from database.database_manager import GestorBaseDatos

logger = logging.getLogger(__name__)

class ProcesadorImagenes:
    def __init__(self):
        self.gestor_bd = GestorBaseDatos()
        
        # Crear directorios
        os.makedirs(DIRECTORIO_SUBIDAS, exist_ok=True)
        os.makedirs(DIRECTORIO_ZIP, exist_ok=True)
        
        # Iniciar limpieza periódica de nodos inactivos
        self.iniciar_limpieza_periodica()
    
    def iniciar_limpieza_periodica(self):
        """Iniciar el sistema de limpieza periódica de nodos inactivos"""
        def limpiar_nodos_inactivos():
            """Marcar como inactivos los nodos que no han dado latido en más de 2 minutos"""
            while True:
                try:
                    filas_afectadas = self.gestor_bd.limpiar_nodos_inactivos()
                    
                    if filas_afectadas > 0:
                        logger.info(f"Limpieza automática: {filas_afectadas} nodos marcados como inactivos")
                    
                except Exception as e:
                    logger.error(f"Error en limpieza de nodos: {e}")
                
                time.sleep(INTERVALO_LIMPIEZA)
        
        # Iniciar hilo de limpieza
        hilo_limpieza = threading.Thread(target=limpiar_nodos_inactivos, daemon=True)
        hilo_limpieza.start()
        logger.info("Sistema de limpieza periódica de nodos iniciado")
    
    def archivo_permitido(self, nombre_archivo: str) -> bool:
        """Verificar si la extensión del archivo está permitida"""
        return '.' in nombre_archivo and \
               nombre_archivo.rsplit('.', 1)[1].lower() in EXTENSIONES_PERMITIDAS
    
    def seleccionar_nodo(self, nodos: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Seleccionar el mejor nodo disponible usando balanceo de carga simple"""
        if not nodos:
            return None
        
        # Estrategia simple: seleccionar aleatoriamente entre nodos activos
        return random.choice(nodos)
    
    def enviar_imagen_a_nodo(self, nodo: Dict[str, Any], id_trabajo: str, 
                            ruta_imagen: str, datos_imagen: bytes, 
                            transformaciones: List[str], 
                            parametros: List[Dict[str, str]]) -> Any:
        """Enviar imagen al nodo via gRPC"""
        try:
            # Crear conexión gRPC
            canal = grpc.insecure_channel(f"{nodo['host']}:{nodo['puerto']}")
            stub = servicioimagen_pb2_grpc.ServicioProcesamientoImagenStub(canal)
            
            # Obtener formato de imagen
            formato_imagen = os.path.splitext(ruta_imagen)[1][1:].lower()
            
            # Preparar metadatos con transformaciones
            metadatos = {
                'transformaciones': json.dumps(transformaciones),
                'tiempo_subida': str(time.time())
            }
            
            # Preparar parámetros para gRPC
            params = [
                servicioimagen_pb2.ParametroTransformacion(nombre=k, valor=v)
                for param in parametros
                for k, v in param.items()
            ]
            
            logger.info(f"Enviando imagen al nodo {nodo['nombre']}: Trabajo {id_trabajo}, "
                       f"Transformaciones: {transformaciones}, Parámetros: {parametros}")
            
            # Crear request
            solicitud = servicioimagen_pb2.SolicitudImagen(
                id_trabajo=id_trabajo,
                nombre_imagen=os.path.basename(ruta_imagen),
                datos_imagen=datos_imagen,
                formato_imagen=formato_imagen,
                metadatos=metadatos,
                transformaciones=transformaciones,
                parametros=params
            )
            
            # Enviar imagen con timeout
            respuesta = stub.ProcesarImagen(solicitud, timeout=TIMEOUT_GRPC)
            
            canal.close()
            logger.info(f"Respuesta del nodo {nodo['nombre']}: Exito={respuesta.exito}, "
                       f"Mensaje={respuesta.mensaje}")
            return respuesta
            
        except grpc.RpcError as e:
            logger.error(f"Error gRPC enviando imagen al nodo {nodo['nombre']}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error enviando imagen al nodo {nodo['nombre']}: {e}")
            return None
    
    def procesar_imagen_async(self, nodo: Dict[str, Any], id_trabajo: str, 
                             ruta_imagen: str, datos_imagen: bytes, 
                             transformaciones: List[str], 
                             parametros: List[Dict[str, str]]):
        """Procesar imagen de forma asíncrona"""
        respuesta = self.enviar_imagen_a_nodo(nodo, id_trabajo, ruta_imagen, 
                                             datos_imagen, transformaciones, 
                                             parametros)
        
        if respuesta and respuesta.exito:
            logger.info(f"Imagen procesada exitosamente - Trabajo: {id_trabajo}, Nodo: {nodo['nombre']}")
        else:
            logger.error(f"Error procesando imagen - Trabajo: {id_trabajo}, Nodo: {nodo['nombre']}")
            
            # Marcar trabajo como fallido
            self.gestor_bd.marcar_trabajo_fallido(
                id_trabajo, f"Error en el nodo {nodo['nombre']}"
            )
    
    def subir_imagen(self, archivo, transformaciones: List[str], 
                     parametros: List[Dict[str, str]]) -> Tuple[Dict[str, Any], int]:
        """Subir y procesar imagen con transformaciones y parámetros"""
        try:
            # Validar archivo
            if not archivo or archivo.filename == '':
                return {'error': 'No se seleccionó archivo'}, 400
            
            if not self.archivo_permitido(archivo.filename):
                return {'error': 'Tipo de archivo no permitido'}, 400
            
            # Validar transformaciones
            transformaciones_validas = [
                t for t in transformaciones if t.lower() in TRANSFORMACIONES_SOPORTADAS
            ]
            logger.info(f"Transformaciones válidas para {archivo.filename}: {transformaciones_validas}")
            logger.info(f"Parámetros para {archivo.filename}: {parametros}")
            
            # Guardar archivo
            nombre_archivo = secure_filename(archivo.filename)
            marca_tiempo = str(int(time.time()))
            nombre_archivo_unico = f"{marca_tiempo}_{nombre_archivo}"
            ruta_archivo = os.path.join(DIRECTORIO_SUBIDAS, nombre_archivo_unico)
            archivo.save(ruta_archivo)
            
            # Obtener tamaño del archivo
            tamano_archivo = os.path.getsize(ruta_archivo)
            
            # Leer datos del archivo
            with open(ruta_archivo, 'rb') as f:
                datos_imagen = f.read()
            
            # Obtener nodos disponibles
            nodos_disponibles = self.gestor_bd.obtener_nodos_disponibles()
            if not nodos_disponibles:
                return {'error': 'No hay nodos disponibles'}, 503
            
            # Seleccionar nodo
            nodo_seleccionado = self.seleccionar_nodo(nodos_disponibles)
            
            # Crear registro de trabajo
            logger.info(f"Creando registro de trabajo para {nombre_archivo_unico} con id_nodo={nodo_seleccionado['id']}")
            id_trabajo = self.gestor_bd.crear_registro_trabajo(
                nombre_archivo_unico, tamano_archivo, 
                transformaciones_validas, nodo_seleccionado['id'], 
                None, parametros
            )
            if not id_trabajo:
                return {'error': 'Error creando trabajo'}, 500
            
            # Procesar imagen de forma asíncrona
            hilo_procesamiento = threading.Thread(
                target=self.procesar_imagen_async,
                args=(nodo_seleccionado, id_trabajo, ruta_archivo, 
                     datos_imagen, transformaciones_validas, parametros)
            )
            hilo_procesamiento.daemon = True
            hilo_procesamiento.start()
            
            return {
                'exito': True,
                'id_trabajo': id_trabajo,
                'mensaje': 'Imagen enviada para procesamiento',
                'nodo_asignado': nodo_seleccionado['nombre'],
                'nombre_archivo': nombre_archivo_unico,
                'transformaciones_aplicadas': transformaciones_validas,
                'parametros': parametros
            }, 202
            
        except Exception as e:
            logger.error(f"Error subiendo imagen: {e}")
            return {'error': 'Error interno del servidor'}, 500
    
    def subir_lote(self, archivos, configuraciones: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], int]:
        """Subir y procesar un lote de imágenes con configuraciones específicas"""
        try:
            if not archivos:
                return {'error': 'No se proporcionaron archivos'}, 400
            
            if len(archivos) != len(configuraciones):
                return {'error': 'El número de archivos y configuraciones no coincide'}, 400
            
            batch_id = str(uuid.uuid4())
            resultados = []
            nodos_disponibles = self.gestor_bd.obtener_nodos_disponibles()
            
            if not nodos_disponibles:
                return {'error': 'No hay nodos disponibles'}, 503

            for archivo, config in zip(archivos, configuraciones):
                if not self.archivo_permitido(archivo.filename):
                    logger.warning(f"Archivo no permitido: {archivo.filename}")
                    continue
                
                # Validar transformaciones
                transformaciones = config.get('transformaciones', [])
                transformaciones_validas = [
                    t for t in transformaciones if t.lower() in TRANSFORMACIONES_SOPORTADAS
                ]
                logger.info(f"Transformaciones para {archivo.filename}: {transformaciones}")
                logger.info(f"Transformaciones válidas: {transformaciones_validas}")
                
                # Obtener parámetros
                parametros = config.get('parametros', {})
                logger.info(f"Parámetros para {archivo.filename}: {parametros}")
                
                # Guardar archivo
                nombre_archivo = secure_filename(archivo.filename)
                marca_tiempo = str(int(time.time()))
                nombre_archivo_unico = f"{marca_tiempo}_{nombre_archivo}"
                ruta_archivo = os.path.join(DIRECTORIO_SUBIDAS, nombre_archivo_unico)
                archivo.save(ruta_archivo)
                
                # Obtener tamaño del archivo
                tamano_archivo = os.path.getsize(ruta_archivo)
                
                # Leer datos del archivo
                with open(ruta_archivo, 'rb') as f:
                    datos_imagen = f.read()
                
                # Seleccionar nodo
                nodo_seleccionado = self.seleccionar_nodo(nodos_disponibles)
                
                # Crear registro de trabajo
                id_trabajo = self.gestor_bd.crear_registro_trabajo(
                    nombre_archivo_unico, tamano_archivo, 
                    transformaciones_validas, nodo_seleccionado['id'], 
                    batch_id, [parametros]
                )
                if not id_trabajo:
                    logger.error(f"Error creando trabajo para {nombre_archivo_unico}")
                    continue
                
                # Procesar imagen de forma asíncrona
                hilo_procesamiento = threading.Thread(
                    target=self.procesar_imagen_async,
                    args=(nodo_seleccionado, id_trabajo, ruta_archivo, 
                         datos_imagen, transformaciones_validas, [parametros])
                )
                hilo_procesamiento.daemon = True
                hilo_procesamiento.start()
                
                resultados.append({
                    'id_trabajo': id_trabajo,
                    'nombre_archivo': nombre_archivo_unico,
                    'nodo_asignado': nodo_seleccionado['nombre'],
                    'transformaciones': transformaciones_validas,
                    'parametros': parametros
                })
            
            logger.info(f"Lote {batch_id} creado con {len(resultados)} trabajos")
            return {
                'exito': True,
                'batch_id': batch_id,
                'mensaje': 'Lote enviado para procesamiento',
                'trabajos': resultados
            }, 202
            
        except Exception as e:
            logger.error(f"Error subiendo lote: {e}")
            return {'error': 'Error interno del servidor'}, 500
    
    def obtener_estado_trabajo(self, id_trabajo: str) -> Tuple[Dict[str, Any], int]:
        """Obtener estado de un trabajo"""
        try:
            trabajo = self.gestor_bd.obtener_trabajo(id_trabajo)
            
            if not trabajo:
                return {'error': 'Trabajo no encontrado'}, 404
            
            return trabajo, 200
            
        except Exception as e:
            logger.error(f"Error obteniendo estado del trabajo: {e}")
            return {'error': 'Error interno del servidor'}, 500
    
    def obtener_estado_lote(self, batch_id: str) -> Tuple[Dict[str, Any], int]:
        """Obtener estado de todos los trabajos de un lote"""
        try:
            trabajos = self.gestor_bd.obtener_trabajos_por_lote(batch_id)
            
            if not trabajos:
                return {'error': 'Lote no encontrado'}, 404
            
            return {
                'batch_id': batch_id,
                'trabajos': trabajos,
                'estado_general': self._calcular_estado_general(trabajos)
            }, 200
            
        except Exception as e:
            logger.error(f"Error obteniendo estado del lote: {e}")
            return {'error': 'Error interno del servidor'}, 500
    
    def _calcular_estado_general(self, trabajos: List[Dict[str, Any]]) -> str:
        """Calcular el estado general del lote"""
        estados = {t['estado'] for t in trabajos}
        if 'fallido' in estados:
            return 'fallido'
        if 'procesando' in estados or 'pendiente' in estados:
            return 'procesando'
        if all(t['estado'] == 'completado' for t in trabajos):
            return 'completado'
        return 'desconocido'
    
    def obtener_estado_nodos(self) -> Tuple[List[Dict[str, Any]], int]:
        """Obtener estado de todos los nodos"""
        try:
            nodos = self.gestor_bd.obtener_nodos()
            return nodos, 200
            
        except Exception as e:
            logger.error(f"Error obteniendo estado de nodos: {e}")
            return {'error': 'Error interno del servidor'}, 500
    
    def limpiar_nodos_inactivos_manual(self) -> int:
        """Limpieza manual de nodos inactivos (para llamadas API)"""
        return self.gestor_bd.limpiar_nodos_inactivos()
    
    def descargar_lote(self, batch_id: str) -> Tuple[str, str, Optional[Dict]]:
        """Preparar descarga de un lote como archivo ZIP"""
        try:
            trabajos = self.gestor_bd.obtener_trabajos_por_lote(batch_id)
            
            if not trabajos:
                return None, None, {'error': 'Lote no encontrado'}
            
            if not all(t['estado'] == 'completado' for t in trabajos):
                return None, None, {'error': 'No todos los trabajos del lote están completados'}
            
            zip_nombre = f"lote_{batch_id}.zip"
            zip_ruta = os.path.join(DIRECTORIO_ZIP, zip_nombre)
            os.makedirs(DIRECTORIO_ZIP, exist_ok=True)
            
            archivos_encontrados = []
            with zipfile.ZipFile(zip_ruta, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for trabajo in trabajos:
                    nombre_archivo_resultado = os.path.basename(trabajo['ruta_resultado'])
                    nombre_nodo = trabajo['nombre_nodo']
                    
                    # FIX: Usa ruta absoluta desde config (ajusta si hay múltiples nodos)
                    base_nodos = DIRECTORIO_RESULTADOS_NODOS  # De config.py
                    ruta_real = os.path.normpath(
                        os.path.join(base_nodos, "resultados", nombre_nodo, nombre_archivo_resultado)
                    )
                    
                    # Log temporal para confirmar
                    logger.info(f"DEBUG - Buscando absoluta: {ruta_real} (existe? {os.path.exists(ruta_real)})")
                    
                    if not os.path.exists(ruta_real):
                        logger.error(f"Archivo NO encontrado: {ruta_real}")
                        continue
                    
                    # Agregar al ZIP
                    nombre_en_zip = os.path.splitext(trabajo['nombre_imagen'])[0] + '_procesada.' + os.path.splitext(nombre_archivo_resultado)[1]
                    zipf.write(ruta_real, nombre_en_zip)
                    archivos_encontrados.append(ruta_real)
                    logger.info(f"Archivo agregado: {nombre_en_zip}")
            
            if not archivos_encontrados:
                logger.error(f"No se encontraron archivos para el lote {batch_id}")
                if os.path.exists(zip_ruta):
                    os.remove(zip_ruta)
                return None, None, {'error': 'No se encontraron archivos procesados para el lote'}
            
            logger.info(f"ZIP creado: {zip_ruta} con {len(archivos_encontrados)} archivos")
            return zip_ruta, zip_nombre, None
            
        except Exception as e:
            logger.error(f"Error descargando lote: {e}")
            if 'zip_ruta' in locals() and os.path.exists(zip_ruta):
                os.remove(zip_ruta)
            return None, None, {'error': f'Error interno: {str(e)}'}
        
    def descargar_resultado(self, id_trabajo: str):
        trabajo = self.gestor_bd.obtener_trabajo(id_trabajo)
        if not trabajo:
            return None, {'error': 'Trabajo no encontrado'}, 404
        if trabajo['estado'] != 'completado':
            return None, {'error': 'El trabajo aún no está completado'}, 400
        
        nombre_archivo = os.path.basename(trabajo['ruta_resultado'])  # Basename
        nombre_nodo = trabajo['nombre_nodo']
        ruta_real = os.path.normpath(
            os.path.join(DIRECTORIO_RESULTADOS_NODOS, "resultados", nombre_nodo, nombre_archivo)
        )
        
        logger.info(f"DEBUG - Descarga individual: {ruta_real} (existe? {os.path.exists(ruta_real)})")
        
        if not os.path.exists(ruta_real):
            return None, {'error': 'Archivo de resultado no encontrado', 'ruta_buscada': ruta_real}, 404
        
        return ruta_real, os.path.basename(trabajo['nombre_imagen']), None