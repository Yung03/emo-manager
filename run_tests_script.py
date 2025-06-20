#!/usr/bin/env python3
"""
Script para ejecutar tests del EMO Manager con diferentes configuraciones.
"""

import subprocess
import sys
import time
from pathlib import Path

def run_command(cmd, description):
    """Ejecuta un comando y reporta el resultado."""
    print(f"\n🔍 {description}")
    print("=" * 60)
    
    start_time = time.time()
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        execution_time = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ {description} - EXITOSO ({execution_time:.1f}s)")
            if result.stdout:
                print(result.stdout)
        else:
            print(f"❌ {description} - FALLÓ ({execution_time:.1f}s)")
            if result.stderr:
                print("ERRORES:")
                print(result.stderr)
            if result.stdout:
                print("SALIDA:")
                print(result.stdout)
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - TIMEOUT (>5 minutos)")
        return False
    except Exception as e:
        print(f"💥 {description} - ERROR: {e}")
        return False

def check_dependencies():
    """Verifica que las dependencias estén instaladas."""
    print("🔍 VERIFICANDO DEPENDENCIAS...")
    
    required_packages = ['pandas', 'pytest', 'psutil', 'openpyxl']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} - NO INSTALADO")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n🚨 INSTALAR DEPENDENCIAS FALTANTES:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

def main():
    """Función principal para ejecutar tests."""
    
    print("🧪 EMO MANAGER - SUITE DE TESTS")
    print("=" * 50)
    
    # Verificar dependencias
    if not check_dependencies():
        return 1
    
    # Verificar que existen los archivos necesarios
    required_files = ['emo_manager.py', 'test_emo_comprehensive.py']
    missing_files = []
    
    for file in required_files:
        if not Path(file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"\n❌ ARCHIVOS FALTANTES: {missing_files}")
        return 1
    
    print(f"\n📁 Archivos encontrados:")
    for file in Path(".").glob("*.py"):
        print(f"  • {file.name}")
    
    # Ejecutar diferentes tipos de tests
    test_configs = [
        {
            "cmd": "python -m pytest test_emo_comprehensive.py::TestEMOManagerBasic -v",
            "desc": "TESTS BÁSICOS"
        },
        {
            "cmd": "python -m pytest test_emo_comprehensive.py::TestEMOManagerCriticalIssues -v",
            "desc": "TESTS DE ISSUES CRÍTICOS"
        },
        {
            "cmd": "python -m pytest test_emo_comprehensive.py::TestEMOManagerAnalysis -v",
            "desc": "TESTS DE ANÁLISIS"
        },
        {
            "cmd": "python -m pytest test_emo_comprehensive.py::TestEMOManagerQuality -v",
            "desc": "TESTS DE CALIDAD"
        },
        {
            "cmd": "python -m pytest test_emo_comprehensive.py::TestEMOManagerExcelExport -v",
            "desc": "TESTS DE EXPORTACIÓN EXCEL"
        },
        {
            "cmd": "python -m pytest test_emo_comprehensive.py::TestEMOManagerAdvancedFeatures -v",
            "desc": "TESTS DE FUNCIONES AVANZADAS"
        }
    ]
    
    # Ejecutar tests básicos
    print(f"\n🚀 EJECUTANDO TESTS PRINCIPALES...")
    
    passed_tests = 0
    failed_tests = 0
    
    for config in test_configs:
        success = run_command(config["cmd"], config["desc"])
        if success:
            passed_tests += 1
        else:
            failed_tests += 1
    
    # Tests de performance (opcionales)
    print(f"\n⚡ TESTS DE PERFORMANCE (OPCIONALES)...")
    
    performance_cmd = "python -m pytest test_emo_comprehensive.py -m slow -v"
    performance_success = run_command(performance_cmd, "TESTS DE PERFORMANCE")
    
    # Test de funcionamiento básico del manager
    print(f"\n🏥 TEST DE FUNCIONAMIENTO BÁSICO...")
    basic_test_success = run_command("python emo_manager.py", "EJECUCIÓN DIRECTA DEL MANAGER")
    
    # Resumen final
    print(f"\n📊 RESUMEN DE RESULTADOS")
    print("=" * 50)
    print(f"✅ Tests exitosos: {passed_tests}")
    print(f"❌ Tests fallidos: {failed_tests}")
    print(f"⚡ Performance test: {'✅ EXITOSO' if performance_success else '❌ FALLÓ'}")
    print(f"🏥 Test básico manager: {'✅ EXITOSO' if basic_test_success else '❌ FALLÓ'}")
    
    if failed_tests == 0 and basic_test_success:
        print(f"\n🎉 ¡TODOS LOS TESTS PRINCIPALES PASARON!")
        print(f"✅ EMO Manager está funcionando correctamente")
        return 0
    else:
        print(f"\n🚨 ALGUNOS TESTS FALLARON")
        print(f"💡 Revisa los errores específicos arriba")
        return 1

if __name__ == "__main__":
    exit_code = main()
    
    print(f"\n🔚 TESTS COMPLETADOS")
    if exit_code == 0:
        print("🎯 ¡TODO FUNCIONANDO CORRECTAMENTE!")
    else:
        print("🔧 NECESITA CORRECCIONES")
    
    sys.exit(exit_code)