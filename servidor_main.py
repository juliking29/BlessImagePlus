#!/usr/bin/env python3
"""
Servidor principal que recibe imágenes y las distribuye a los nodos
Versión refactorizada - más corto y organizado
"""

import time
import logging
import json
from flask import Flask, request, jsonify, send_file

from config.config import HOST, PUERTO, DEBUG, MAX_CONTENT_LENGTH, TRANSFORMACIONES_SOPORTADAS
from image_processing.image_processor import ProcesadorImagenes

# Configurar logging
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Instancia global del procesador de imágenes
procesador = ProcesadorImagenes()

# ===================== RUTAS DE LA API =====================

@app.route('/subir', methods=['POST'])
def subir_imagen():
    """Subir imagen para procesamiento con transformaciones"""
    if 'imagen' not in request.files:
        return jsonify({'error': 'No se encontró archivo de imagen'}), 400
    
    archivo = request.files['imagen']
    transformaciones = request.form.getlist('transformaciones')
    if not transformaciones:
        transformaciones = []
    
    # Recolectar parámetros de los campos que comienzan con 'param_'
    parametros = {
        key: value for key, value in request.form.items()
        if key.startswith('param_')
    }
    logger.info(f"Parámetros recibidos para {archivo.filename}: {parametros}")
    
    resultado, codigo_estado = procesador.subir_imagen(archivo, transformaciones, [parametros])
    return jsonify(resultado), codigo_estado

@app.route('/subir_lote', methods=['POST'])
def subir_lote():
    """Subir un lote de imágenes para procesamiento"""
    if 'imagenes' not in request.files:
        return jsonify({'error': 'No se encontraron archivos de imagen'}), 400
    
    archivos = request.files.getlist('imagenes')
    try:
        configuraciones = json.loads(request.form.get('configuraciones', '[]'))
        logger.info(f"Configuraciones recibidas: {configuraciones}")
    except json.JSONDecodeError:
        return jsonify({'error': 'Formato de configuraciones inválido'}), 400
    
    resultado, codigo_estado = procesador.subir_lote(archivos, configuraciones)
    return jsonify(resultado), codigo_estado

@app.route('/estado/<id_trabajo>', methods=['GET'])
def obtener_estado_trabajo(id_trabajo):
    """Obtener estado de un trabajo"""
    resultado, codigo_estado = procesador.obtener_estado_trabajo(id_trabajo)
    return jsonify(resultado), codigo_estado

@app.route('/estado_lote/<batch_id>', methods=['GET'])
def obtener_estado_lote(batch_id):
    """Obtener estado de un lote"""
    resultado, codigo_estado = procesador.obtener_estado_lote(batch_id)
    return jsonify(resultado), codigo_estado

@app.route('/nodos', methods=['GET'])
def obtener_estado_nodos():
    """Obtener estado de todos los nodos"""
    resultado, codigo_estado = procesador.obtener_estado_nodos()
    return jsonify(resultado), codigo_estado

@app.route('/nodos/limpiar', methods=['POST'])
def limpiar_nodos_inactivos():
    """Forzar limpieza de nodos inactivos"""
    try:
        nodos_limpiados = procesador.limpiar_nodos_inactivos_manual()
        return jsonify({
            'exito': True,
            'mensaje': f'Se limpiaron {nodos_limpiados} nodos inactivos'
        }), 200
    except Exception as e:
        return jsonify({'error': f'Error limpiando nodos: {str(e)}'}), 500

@app.route('/descargar/<id_trabajo>', methods=['GET'])
def descargar_resultado(id_trabajo):
    """Descargar resultado procesado"""
    try:
        ruta_real, nombre_archivo, error = procesador.descargar_resultado(id_trabajo)
        
        if error:
            return jsonify(error[0]), error[1]
        
        return send_file(ruta_real, as_attachment=True, download_name=nombre_archivo)
        
    except Exception as e:
        logger.error(f"Error descargando resultado: {e}")
        return jsonify({'error': 'Error interno del servidor'}), 500

@app.route('/descargar_lote/<batch_id>', methods=['GET'])
def descargar_lote(batch_id):
    """Descargar resultados de un lote como archivo ZIP"""
    try:
        ruta_real, nombre_archivo, error = procesador.descargar_lote(batch_id)
        
        if error:  # Si error es dict (no None)
            return jsonify(error), 400 if 'no encontrados' in error.get('error', '') else 500  # Código específico
        
        return send_file(ruta_real, as_attachment=True, download_name=nombre_archivo)
        
    except Exception as e:
        logger.error(f"Error descargando lote: {e}")
        return jsonify({'error': 'Error interno del servidor'}), 500

@app.route('/transformaciones', methods=['GET'])
def obtener_transformaciones():
    """Obtener lista de transformaciones soportadas"""
    return jsonify({
        'transformaciones_soportadas': list(TRANSFORMACIONES_SOPORTADAS)
    })

@app.route('/salud', methods=['GET'])
def verificar_salud():
    """Verificación de salud del servidor"""
    return jsonify({
        'estado': 'saludable',
        'marca_tiempo': time.time(),
        'version': '1.0.0'
    })

@app.route('/', methods=['GET'])
def inicio():
    """Página principal con información de la API"""
    return jsonify({
        'mensaje': 'Servidor de Procesamiento de Imágenes Distribuido',
        'endpoints': {
            'POST /subir': 'Subir imagen para procesamiento con transformaciones',
            'POST /subir_lote': 'Subir un lote de imágenes para procesamiento',
            'GET /estado/<id_trabajo>': 'Obtener estado de un trabajo',
            'GET /estado_lote/<batch_id>': 'Obtener estado de un lote',
            'GET /nodos': 'Obtener estado de todos los nodos',
            'POST /nodos/limpiar': 'Forzar limpieza de nodos inactivos',
            'GET /descargar/<id_trabajo>': 'Descargar resultado procesado',
            'GET /descargar_lote/<batch_id>': 'Descargar resultados de un lote como ZIP',
            'GET /transformaciones': 'Obtener transformaciones soportadas',
            'GET /salud': 'Verificar salud del servidor'
        }
    })

# ===================== PUNTO DE ENTRADA =====================

if __name__ == '__main__':
    logger.info("Iniciando servidor principal de procesamiento de imágenes...")
    logger.info(f"Servidor ejecutándose en http://{HOST}:{PUERTO}")
    app.run(host=HOST, port=PUERTO, debug=DEBUG)