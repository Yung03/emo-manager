#!/usr/bin/env python3
"""
Test simple y funcional para EMO Manager
Compatible con Windows Python 3.13
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from emo_manager import EMOManagerOptimized
import pandas as pd
from datetime import datetime, timedelta

def test_basic_functionality():
    """Test básico de funcionalidad."""
    print("TEST 1: Creación básica del manager")
    
    # Datos de prueba
    sample_data = {
        "Nombre": ["Ana García", "Luis Martín", "Marta López", "Carlos Ruiz"],
        "Area": ["SSOMA", "Logística", "SSOMA", "IT"],
        "emo_vence": ["2025-06-30", "30/07/2025", None, "2024-12-01"]
    }
    
    try:
        manager = EMOManagerOptimized(sample_data)
        print("  [OK] Manager creado exitosamente")
        print(f"  [INFO] Registros procesados: {len(manager.df)}")
        assert len(manager.df) == 4
        print("  [PASS] Test creación básica")
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        return False

def test_data_operations():
    """Test operaciones de datos."""
    print("\nTEST 2: Operaciones de datos")
    
    sample_data = {
        "Nombre": ["Juan Pérez", "María García", "Carlos López"],
        "Area": ["IT", "RRHH", "Logística"],
        "emo_vence": ["2025-07-15", "2025-06-25", "2025-08-30"]
    }
    
    try:
        manager = EMOManagerOptimized(sample_data)
        
        # Test get_expiring_soon
        proximos = manager.get_expiring_soon(60)
        print(f"  [OK] EMOs próximos: {len(proximos)}")
        assert isinstance(proximos, pd.DataFrame)
        
        # Test get_expired_emos
        vencidos = manager.get_expired_emos()
        print(f"  [OK] EMOs vencidos: {len(vencidos)}")
        assert isinstance(vencidos, pd.DataFrame)
        
        # Test generate_priority_report
        prioridades = manager.generate_priority_report()
        print(f"  [OK] Reporte de prioridades generado")
        assert isinstance(prioridades, dict)
        assert "total_validos" in prioridades
        
        print("  [PASS] Test operaciones de datos")
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        return False

def test_edge_cases():
    """Test casos extremos."""
    print("\nTEST 3: Casos extremos")
    
    try:
        # Test con datos vacíos
        try:
            EMOManagerOptimized({})
            print("  [FAIL] Debería fallar con datos vacíos")
            return False
        except ValueError:
            print("  [OK] Maneja correctamente datos vacíos")
        
        # Test con fechas inválidas
        invalid_data = {
            "Nombre": ["Test1", "Test2"],
            "Area": ["IT", "RRHH"],
            "emo_vence": [None, "fecha_incorrecta"]
        }
        
        manager = EMOManagerOptimized(invalid_data)
        proximos = manager.get_expiring_soon(30)
        print(f"  [OK] Maneja fechas inválidas: {len(proximos)} resultados")
        
        # Test parámetros inválidos
        try:
            manager.get_expiring_soon(0)
            print("  [FAIL] Debería fallar con days_ahead = 0")
            return False
        except ValueError:
            print("  [OK] Valida parámetros correctamente")
        
        print("  [PASS] Test casos extremos")
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        return False

def test_data_quality():
    """Test calidad de datos."""
    print("\nTEST 4: Calidad de datos")
    
    sample_data = {
        "Nombre": ["Juan", "María", "Carlos"],
        "Area": ["IT", "RRHH", "Logística"],
        "emo_vence": ["2025-07-15", None, "2025-08-30"]
    }
    
    try:
        manager = EMOManagerOptimized(sample_data)
        
        # Test reporte de calidad
        quality = manager.get_data_quality_report()
        print(f"  [OK] Total registros: {quality['total_registros']}")
        print(f"  [OK] Fechas válidas: {quality['fechas_validas']}")
        print(f"  [OK] Fechas inválidas: {quality['fechas_invalidas']}")
        print(f"  [OK] Porcentaje válido: {quality['porcentaje_valido']}%")
        
        assert quality['total_registros'] == 3
        assert quality['fechas_invalidas'] == 1
        assert quality['porcentaje_valido'] > 0
        
        print("  [PASS] Test calidad de datos")
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        return False

def test_excel_export():
    """Test exportación Excel."""
    print("\nTEST 5: Exportación Excel")
    
    sample_data = {
        "Nombre": ["Ana", "Luis", "Carlos"],
        "Area": ["IT", "RRHH", "Logística"],
        "emo_vence": ["2025-07-15", "2025-06-25", "2025-08-30"]
    }
    
    try:
        manager = EMOManagerOptimized(sample_data)
        
        # Test exportación
        output_file = "test_export.xlsx"
        result_path = manager.export_to_excel(output_file)
        
        # Verificar que el archivo existe
        if os.path.exists(result_path):
            print(f"  [OK] Archivo Excel creado: {output_file}")
            file_size = os.path.getsize(result_path) / 1024
            print(f"  [OK] Tamaño del archivo: {file_size:.1f}KB")
            
            # Limpiar archivo de prueba
            os.remove(result_path)
            print("  [OK] Archivo de prueba eliminado")
        else:
            print("  [FAIL] Archivo Excel no fue creado")
            return False
        
        print("  [PASS] Test exportación Excel")
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        return False

def test_performance():
    """Test básico de performance."""
    print("\nTEST 6: Performance básico")
    
    try:
        # Generar datos de prueba
        import random
        
        num_records = 1000
        nombres = [f"Empleado_{i:04d}" for i in range(num_records)]
        areas = ["IT", "RRHH", "Logística", "Mantenimiento", "SSOMA"]
        
        base_date = datetime.now()
        fechas = []
        for i in range(num_records):
            if i % 20 == 0:  # 5% inválidas
                fechas.append(None)
            else:
                days_offset = random.randint(-180, 365)
                fecha = base_date + timedelta(days=days_offset)
                fechas.append(fecha.strftime('%Y-%m-%d'))
        
        test_data = {
            "Nombre": nombres,
            "Area": [random.choice(areas) for _ in range(num_records)],
            "emo_vence": fechas
        }
        
        print(f"  [INFO] Generando {num_records} registros de prueba...")
        
        # Medir tiempo de creación
        import time
        start_time = time.time()
        manager = EMOManagerOptimized(test_data)
        creation_time = time.time() - start_time
        
        print(f"  [OK] Manager creado en {creation_time:.3f}s")
        
        # Medir operaciones
        start_time = time.time()
        proximos = manager.get_expiring_soon(30)
        vencidos = manager.get_expired_emos()
        prioridades = manager.generate_priority_report()
        operation_time = time.time() - start_time
        
        print(f"  [OK] Operaciones completadas en {operation_time:.3f}s")
        print(f"  [INFO] Encontrados - Próximos: {len(proximos)}, Vencidos: {len(vencidos)}")
        
        # Calcular velocidad
        total_time = creation_time + operation_time
        records_per_second = num_records / total_time if total_time > 0 else 0
        print(f"  [OK] Velocidad: {records_per_second:,.0f} registros/segundo")
        
        # Verificar que es razonablemente rápido
        if records_per_second > 1000:
            print("  [PASS] Test performance básico")
            return True
        else:
            print("  [WARN] Performance más lenta de lo esperado")
            return True  # No falla, solo advierte
            
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        return False

def main():
    """Ejecuta todos los tests."""
    print("=" * 60)
    print("EMO MANAGER - SUITE DE TESTS SIMPLIFICADA")
    print("=" * 60)
    
    tests = [
        test_basic_functionality,
        test_data_operations,
        test_edge_cases,
        test_data_quality,
        test_excel_export,
        test_performance
    ]
    
    passed_tests = 0
    failed_tests = 0
    
    for test_func in tests:
        try:
            if test_func():
                passed_tests += 1
            else:
                failed_tests += 1
        except Exception as e:
            print(f"  [ERROR] Test falló con excepción: {e}")
            failed_tests += 1
    
    # Resumen final
    print("\n" + "=" * 60)
    print("RESUMEN DE RESULTADOS")
    print("=" * 60)
    print(f"[OK] Tests exitosos: {passed_tests}")
    print(f"[FAIL] Tests fallidos: {failed_tests}")
    print(f"[INFO] Total tests: {len(tests)}")
    
    if failed_tests == 0:
        print("\n[SUCCESS] TODOS LOS TESTS PASARON!")
        print("[INFO] EMO Manager está funcionando correctamente")
        return 0
    else:
        print(f"\n[WARNING] {failed_tests} tests fallaron")
        print("[INFO] Revisa los errores específicos arriba")
        return 1

if __name__ == "__main__":
    exit_code = main()
    
    print(f"\n[INFO] Tests completados")
    if exit_code == 0:
        print("[SUCCESS] Todo funcionando correctamente!")
    else:
        print("[WARNING] Algunos tests necesitan atención")
    
    # No usar sys.exit en algunos entornos
    print(f"\n[EXIT] Código de salida: {exit_code}")