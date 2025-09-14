#!/usr/bin/env python3
"""
Configuración del sistema de procesamiento de imágenes
"""

import os
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración de la base de datos para Aiven
CONFIG_BD = {
    'host': 'mysql-23a1294c-fervegqa11-36aa.g.aivencloud.com',
    'port': 27157,
    'user': 'avnadmin',
    'database': 'sistema_procesamiento_imagenes'
}

# Configuración de archivos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIRECTORIO_SUBIDAS = os.path.join(BASE_DIR, "../subidas")
DIRECTORIO_ZIP = os.path.join(BASE_DIR, "../descargas_zip")
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB máximo

# Extensiones permitidas
EXTENSIONES_PERMITIDAS = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'tif'}

# Transformaciones soportadas
TRANSFORMACIONES_SOPORTADAS = {
    'escala_grises', 'redimensionar', 'recortar', 'rotar', 'reflejar',
    'desenfocar', 'perfilar', 'ajustar_brillo', 'ajustar_contraste',
    'marca_agua', 'convertir_jpg', 'convertir_png', 'convertir_tif'
}

# Configuración de red
HOST = '0.0.0.0'
PUERTO = 5000
DEBUG = True

# Configuración de timeouts
TIMEOUT_GRPC = 300  # 5 minutos
INTERVALO_LIMPIEZA = 30  # 30 segundos
TIMEOUT_NODO_INACTIVO = 2  # 2 minutos

# Pool de conexiones
TAMAÑO_POOL_CONEXIONES = 5
# Ruta base donde los nodos guardan resultados (ajusta a tu setup)
DIRECTORIO_RESULTADOS_NODOS = r"C:\Users\Sebastián Vega\Desktop\Nueva carpeta (4)\nodos_procesamiento"
BASE_RESULTADOS = r"C:\Users\Sebastián Vega\Desktop\Nueva carpeta (4)\nodos_procesamiento\resultados"