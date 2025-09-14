# Manejador de base de datos 
#!/usr/bin/env python3
"""
Gestor de base de datos para el sistema de procesamiento de imágenes
"""

import os
import json
import logging
import mysql.connector
from mysql.connector import pooling
from typing import Dict, List, Any, Optional
from config import CONFIG_BD, TAMAÑO_POOL_CONEXIONES

logger = logging.getLogger(__name__)

class GestorBaseDatos:
    def __init__(self):
        # Copiar configuración base
        self.config_bd_ssl = CONFIG_BD.copy()

        # Ruta absoluta al archivo ca.pem (dentro de /certs o al lado del script)
        ruta_ca = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "ca.pem"))


        if not os.path.exists(ruta_ca):
            logger.error(f"❌ No se encontró el archivo de certificado SSL: {ruta_ca}")
            raise FileNotFoundError(f"No se encontró el archivo de certificado SSL: {ruta_ca}")

        # Configurar conexión SSL
        self.config_bd_ssl.update({
            "ssl_ca": ruta_ca,
            "ssl_verify_cert": True,
            "ssl_verify_identity": False  # cámbialo a True si quieres validar el host también
        })

        # Crear pool de conexiones
        try:
            self.pool_conexiones = pooling.MySQLConnectionPool(
                pool_name="mi_pool",
                pool_size=TAMAÑO_POOL_CONEXIONES,
                **self.config_bd_ssl
            )
            logger.info("✅ Pool de conexiones a la base de datos creado exitosamente")
        except Exception as e:
            logger.error(f"❌ Error creando pool de conexiones: {e}")
            raise

    def obtener_conexion(self):
        """Obtener conexión a la base de datos desde el pool"""
        try:
            return self.pool_conexiones.get_connection()
        except Exception as e:
            logger.error(f"❌ Error obteniendo conexión del pool: {e}")
            raise
    
    def obtener_conexion(self):
        """Obtener conexión a la base de datos desde el pool"""
        try:
            return self.pool_conexiones.get_connection()
        except Exception as e:
            logger.error(f"Error obteniendo conexión del pool: {e}")
            raise
    
    def obtener_nodos_disponibles(self) -> List[Dict[str, Any]]:
        """Obtener nodos disponibles de la base de datos (solo los realmente activos)"""
        try:
            conexion = self.obtener_conexion()
            cursor = conexion.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT id, nombre, host, puerto, estado, ultimo_latido
                FROM nodos 
                WHERE estado = 'activo'
                AND ultimo_latido > DATE_SUB(NOW(), INTERVAL 2 MINUTE)
                ORDER BY ultimo_latido DESC
            """)
            
            nodos = cursor.fetchall()
            conexion.close()
            return nodos
            
        except Exception as e:
            logger.error(f"Error obteniendo nodos: {e}")
            return []
    
    def crear_registro_trabajo(self, nombre_archivo: str, tamano_archivo: int, 
                              transformaciones: List[str], id_nodo: int = None, 
                              batch_id: str = None, 
                              parametros: List[Dict[str, str]] = None) -> Optional[str]:
        """Crear registro de trabajo en la base de datos"""
        import uuid
        uuid_trabajo = str(uuid.uuid4())
        
        try:
            conexion = self.obtener_conexion()
            cursor = conexion.cursor()
            
            cursor.execute("""
                INSERT INTO trabajos (uuid_trabajo, nombre_imagen, tamano_imagen, 
                                    transformaciones, id_nodo_asignado, batch_id, parametros)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (uuid_trabajo, nombre_archivo, tamano_archivo, 
                 json.dumps(transformaciones), id_nodo, batch_id, 
                 json.dumps(parametros) if parametros else None))
            
            conexion.commit()
            conexion.close()
            
            return uuid_trabajo
            
        except Exception as e:
            logger.error(f"Error creando registro de trabajo: {e}")
            return None
    
    def obtener_trabajo(self, id_trabajo: str) -> Optional[Dict[str, Any]]:
        """Obtener información de un trabajo"""
        try:
            conexion = self.obtener_conexion()
            cursor = conexion.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT t.*, n.nombre as nombre_nodo
                FROM trabajos t
                LEFT JOIN nodos n ON t.id_nodo_asignado = n.id
                WHERE t.uuid_trabajo = %s
            """, (id_trabajo,))
            
            trabajo = cursor.fetchone()
            conexion.close()
            
            if trabajo:
                # Convertir datetime a string para JSON
                if trabajo['creado_en']:
                    trabajo['creado_en'] = trabajo['creado_en'].isoformat()
                if trabajo['actualizado_en']:
                    trabajo['actualizado_en'] = trabajo['actualizado_en'].isoformat()
                if trabajo['procesado_en']:
                    trabajo['procesado_en'] = trabajo['procesado_en'].isoformat()
                
                # Parsear transformaciones JSON
                if trabajo['transformaciones']:
                    try:
                        trabajo['transformaciones'] = json.loads(trabajo['transformaciones'])
                    except:
                        trabajo['transformaciones'] = []
                
                # Parsear parámetros JSON
                if trabajo['parametros']:
                    try:
                        trabajo['parametros'] = json.loads(trabajo['parametros'])
                    except:
                        trabajo['parametros'] = []
            
            return trabajo
            
        except Exception as e:
            logger.error(f"Error obteniendo trabajo: {e}")
            return None
    
    def obtener_trabajos_por_lote(self, batch_id: str) -> List[Dict[str, Any]]:
        """Obtener todos los trabajos de un lote"""
        try:
            conexion = self.obtener_conexion()
            cursor = conexion.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT t.*, n.nombre as nombre_nodo
                FROM trabajos t
                LEFT JOIN nodos n ON t.id_nodo_asignado = n.id
                WHERE t.batch_id = %s
            """, (batch_id,))
            
            trabajos = cursor.fetchall()
            conexion.close()
            
            for trabajo in trabajos:
                # Convertir datetime a string para JSON
                if trabajo['creado_en']:
                    trabajo['creado_en'] = trabajo['creado_en'].isoformat()
                if trabajo['actualizado_en']:
                    trabajo['actualizado_en'] = trabajo['actualizado_en'].isoformat()
                if trabajo['procesado_en']:
                    trabajo['procesado_en'] = trabajo['procesado_en'].isoformat()
                
                # Parsear transformaciones JSON
                if trabajo['transformaciones']:
                    try:
                        trabajo['transformaciones'] = json.loads(trabajo['transformaciones'])
                    except:
                        trabajo['transformaciones'] = []
                
                # Parsear parámetros JSON
                if trabajo['parametros']:
                    try:
                        trabajo['parametros'] = json.loads(trabajo['parametros'])
                    except:
                        trabajo['parametros'] = []
            
            return trabajos
            
        except Exception as e:
            logger.error(f"Error obteniendo trabajos del lote: {e}")
            return []
    
    def obtener_nodos(self) -> List[Dict[str, Any]]:
        """Obtener estado de todos los nodos"""
        try:
            conexion = self.obtener_conexion()
            cursor = conexion.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT id, nombre, host, puerto, estado, ultimo_latido,
                       (SELECT COUNT(*) FROM trabajos 
                        WHERE id_nodo_asignado = nodos.id AND estado = 'procesando') as trabajos_activos,
                       (SELECT COUNT(*) FROM trabajos 
                        WHERE id_nodo_asignado = nodos.id AND estado = 'completado') as trabajos_completados,
                       TIMESTAMPDIFF(SECOND, ultimo_latido, NOW()) as segundos_desde_ultimo_latido
                FROM nodos
                ORDER BY estado, ultimo_latido DESC
            """)
            
            nodos = cursor.fetchall()
            conexion.close()
            
            # Convertir datetime a string
            for nodo in nodos:
                if nodo['ultimo_latido']:
                    nodo['ultimo_latido'] = nodo['ultimo_latido'].isoformat()
            
            return nodos
            
        except Exception as e:
            logger.error(f"Error obteniendo nodos: {e}")
            return []
    
    def limpiar_nodos_inactivos(self) -> int:
        """Marcar como inactivos los nodos que no han dado latido en más de 2 minutos"""
        try:
            conexion = self.obtener_conexion()
            cursor = conexion.cursor()
            
            cursor.execute("""
                UPDATE nodos 
                SET estado = 'inactivo' 
                WHERE ultimo_latido < DATE_SUB(NOW(), INTERVAL 2 MINUTE)
                AND estado != 'inactivo'
            """)
            
            filas_afectadas = cursor.rowcount
            conexion.commit()
            conexion.close()
            
            return filas_afectadas
            
        except Exception as e:
            logger.error(f"Error en limpieza de nodos: {e}")
            return 0
    
    def marcar_trabajo_fallido(self, id_trabajo: str, mensaje_error: str):
        """Marcar trabajo como fallido"""
        try:
            conexion = self.obtener_conexion()
            cursor = conexion.cursor()
            cursor.execute("""
                UPDATE trabajos 
                SET estado = 'fallido', mensaje_error = %s
                WHERE uuid_trabajo = %s
            """, (mensaje_error, id_trabajo))
            conexion.commit()
            conexion.close()
        except Exception as e:
            logger.error(f"Error marcando trabajo como fallido: {e}")