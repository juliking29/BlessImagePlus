@echo off
REM === Script para crear la estructura SERVIDOR_IMAGENES_REFACTOR ===

set ROOT=SERVIDOR_IMAGENES_REFACTOR

REM Crear carpetas principales
mkdir %ROOT%
mkdir %ROOT%\config
mkdir %ROOT%\database
mkdir %ROOT%\image_processing

REM Crear archivos en config
echo. > %ROOT%\config\__init__.py
echo # Configuración general del sistema > %ROOT%\config\config.py
echo -----CERTIFICADO SSL----- > %ROOT%\config\ca.pem

REM Crear archivos en database
echo. > %ROOT%\database\__init__.py
echo # Manejador de base de datos > %ROOT%\database\database_manager.py

REM Crear archivos en image_processing
echo. > %ROOT%\image_processing\__init__.py
echo # Lógica central de procesamiento de imágenes > %ROOT%\image_processing\image_processor.py
echo # gRPC generado automáticamente > %ROOT%\image_processing\servicioimagen_pb2.py
echo # gRPC generado automáticamente > %ROOT%\image_processing\servicioimagen_pb2_grpc.py
echo syntax = "proto3"; > %ROOT%\image_processing\servicioimagen.proto

echo ============================================
echo Estructura creada en %ROOT%
echo ============================================

pause
