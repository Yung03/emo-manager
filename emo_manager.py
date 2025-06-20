# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
from pathlib import Path
from functools import lru_cache
import time
import psutil
import os
import sys

# Configurar encoding para Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EMOManagerOptimized:
    """
    Gestor optimizado de vencimientos EMO - VERSIÓN COMPATIBLE CON WINDOWS
    Sin timezone, sin emojis problemáticos, totalmente funcional.
    """
    
    def __init__(self, empleados_data: Dict):
        """
        Inicializa el manager optimizado.
        
        Args:
            empleados_data: Dict con datos de empleados
        """
        if not empleados_data:
            raise ValueError("Los datos de empleados no pueden estar vacíos")
        
        self.df = self._create_dataframe(empleados_data)
        self._optimize_dataframe()
        self._clean_data()
        
        # Performance monitoring
        self.performance_stats = {
            'records_processed': len(self.df),
            'memory_usage_mb': self._get_memory_usage(),
            'creation_time': time.time()
        }
        
        logger.info(f"EMOManager inicializado: {len(self.df)} registros, "
                   f"memoria: {self.performance_stats['memory_usage_mb']:.2f}MB")
    
    def _get_memory_usage(self) -> float:
        """Obtiene uso de memoria actual en MB."""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def _create_dataframe(self, data: Dict) -> pd.DataFrame:
        """Crea DataFrame optimizado con validaciones."""
        required_columns = {'Nombre', 'Area', 'emo_vence'}
        
        if not required_columns.issubset(data.keys()):
            missing = required_columns - data.keys()
            raise ValueError(f"Faltan columnas requeridas: {missing}")
        
        df = pd.DataFrame(data)
        
        # Validar que no esté vacío
        if df.empty:
            raise ValueError("DataFrame no puede estar vacío")
        
        logger.info(f"DataFrame creado con {len(df)} registros")
        return df
    
    def _optimize_dataframe(self):
        """Optimiza el DataFrame para mejor performance."""
        # Convertir strings a category para ahorrar memoria
        if len(self.df) > 1000:
            self.df['Area'] = self.df['Area'].astype('category')
    
    def _clean_data(self):
        """Limpia y normaliza los datos SIN timezone complications."""
        initial_count = len(self.df)
        
        # Eliminar duplicados basado en nombre y área
        self.df = self.df.drop_duplicates(subset=['Nombre', 'Area'], keep='first')
        duplicates_removed = initial_count - len(self.df)
        if duplicates_removed > 0:
            logger.info(f"Duplicados eliminados: {duplicates_removed}")
        
        # Normalizar strings
        self.df["Nombre"] = self.df["Nombre"].str.strip().str.title()
        self.df["Area"] = self.df["Area"].str.strip().str.title()
        
        # SIMPLIFICADO: Convertir fechas SIN timezone
        self.df["emo_vence"] = pd.to_datetime(
            self.df["emo_vence"], 
            errors="coerce",
            format='mixed'
        )
        
        # Contar errores de conversión
        invalid_dates = self.df["emo_vence"].isna().sum()
        if invalid_dates > 0:
            logger.warning(f"Fechas inválidas encontradas: {invalid_dates}")
    
    def _get_current_datetime(self) -> pd.Timestamp:
        """Obtiene datetime actual SIN timezone."""
        return pd.Timestamp.now()
    
    def clear_cache(self):
        """Limpia el cache LRU."""
        self.get_expiring_soon.cache_clear()
        self.get_expired_emos.cache_clear()
        self.generate_priority_report.cache_clear()
        logger.info("Cache limpiado")
    
    @lru_cache(maxsize=128)
    def get_expiring_soon(self, days_ahead: int = 30) -> pd.DataFrame:
        """
        Obtiene EMOs próximos a vencer - VERSIÓN SIMPLIFICADA.
        """
        if days_ahead <= 0:
            raise ValueError("days_ahead debe ser mayor a 0")
        
        start_time = time.time()
        
        hoy = self._get_current_datetime()
        fecha_limite = hoy + pd.Timedelta(days=days_ahead)
        
        # Filtrar solo fechas válidas
        df_validas = self.df.dropna(subset=["emo_vence"]).copy()
        
        if df_validas.empty:
            logger.info("No hay registros con fechas válidas")
            return pd.DataFrame()
        
        # Filtrar rango de fechas
        mask = (df_validas["emo_vence"] >= hoy) & (df_validas["emo_vence"] <= fecha_limite)
        df_expiring = df_validas[mask].copy()
        
        # CORREGIDO: Calcular días restantes de forma más simple
        if not df_expiring.empty:
            df_expiring = df_expiring.copy()  # Asegurar que es una copia
            days_diff = (df_expiring["emo_vence"] - hoy).dt.days
            df_expiring["dias_restantes"] = days_diff
            df_expiring = df_expiring.sort_values("dias_restantes")
        
        execution_time = time.time() - start_time
        logger.info(f"get_expiring_soon ejecutado en {execution_time:.3f}s - "
                   f"Encontrados: {len(df_expiring)} EMOs")
        
        return df_expiring
    
    @lru_cache(maxsize=64)
    def get_expired_emos(self) -> pd.DataFrame:
        """Obtiene EMOs vencidos - VERSIÓN SIMPLIFICADA."""
        start_time = time.time()
        
        hoy = self._get_current_datetime()
        df_validas = self.df.dropna(subset=["emo_vence"]).copy()
        
        if df_validas.empty:
            return pd.DataFrame()
        
        mask = df_validas["emo_vence"] < hoy
        df_expired = df_validas[mask].copy()
        
        # Calcular días vencidos
        if not df_expired.empty:
            df_expired = df_expired.copy()  # Asegurar que es una copia
            days_diff = (hoy - df_expired["emo_vence"]).dt.days
            df_expired["dias_vencido"] = days_diff
            df_expired = df_expired.sort_values("dias_vencido", ascending=False)
        
        execution_time = time.time() - start_time
        logger.info(f"get_expired_emos ejecutado en {execution_time:.3f}s - "
                   f"Encontrados: {len(df_expired)} EMOs vencidos")
        
        return df_expired
    
    @lru_cache(maxsize=32)
    def generate_priority_report(self, days_ahead: int = 90) -> Dict:
        """
        Genera reporte de prioridades - VERSIÓN SIMPLIFICADA.
        """
        hoy = self._get_current_datetime()
        df_validas = self.df.dropna(subset=["emo_vence"]).copy()
        
        if df_validas.empty:
            return {
                "vencidos": 0, "urgente_7_dias": 0, "alta_30_dias": 0,
                "media_90_dias": 0, "baja_mas_90_dias": 0, "total_validos": 0
            }
        
        # Calcular días restantes
        df_validas = df_validas.copy()  # Asegurar que es una copia
        days_diff = (df_validas["emo_vence"] - hoy).dt.days
        df_validas["dias_restantes"] = days_diff
        
        # Categorizar por prioridad
        vencidos = len(df_validas[df_validas["dias_restantes"] < 0])
        urgente = len(df_validas[(df_validas["dias_restantes"] >= 0) & 
                                (df_validas["dias_restantes"] <= 7)])
        alta = len(df_validas[(df_validas["dias_restantes"] > 7) & 
                             (df_validas["dias_restantes"] <= 30)])
        media = len(df_validas[(df_validas["dias_restantes"] > 30) & 
                              (df_validas["dias_restantes"] <= 90)])
        baja = len(df_validas[df_validas["dias_restantes"] > 90])
        
        return {
            "vencidos": vencidos,
            "urgente_7_dias": urgente,
            "alta_30_dias": alta,
            "media_90_dias": media,
            "baja_mas_90_dias": baja,
            "total_validos": len(df_validas)
        }
    
    def generate_report_by_area(self, days_ahead: int = 30) -> pd.DataFrame:
        """Genera reporte agrupado por área."""
        df_expiring = self.get_expiring_soon(days_ahead)
        
        if df_expiring.empty:
            logger.info("No hay EMOs próximos a vencer")
            return pd.DataFrame(columns=["Area", "Cantidad", "Empleados", "Promedio_Dias"])
        
        # Agrupar por área
        reporte = (df_expiring.groupby("Area", as_index=False)
                  .agg({
                      "Nombre": ["count", lambda x: x.tolist()],
                      "dias_restantes": "mean"
                  }))
        
        # Aplanar columnas multi-level
        reporte.columns = ["Area", "Cantidad", "Empleados", "Promedio_Dias"]
        reporte["Promedio_Dias"] = reporte["Promedio_Dias"].round(1)
        
        return reporte.sort_values("Cantidad", ascending=False)
    
    def export_to_excel(self, filepath: str = "reporte_emo.xlsx") -> str:
        """
        Exporta reportes a Excel - VERSIÓN SIMPLIFICADA.
        """
        start_time = time.time()
        
        try:
            filepath = Path(filepath)
            
            # Crear datos para exportar
            export_data = {}
            
            # Hoja 1: Resumen
            priority_data = self.generate_priority_report()
            export_data["Resumen"] = pd.DataFrame([
                {"Metrica": k, "Valor": v} for k, v in priority_data.items()
            ])
            
            # Hoja 2: Próximos a vencer
            proximos = self.get_expiring_soon(30)
            if not proximos.empty:
                export_data["Proximos_30_Dias"] = proximos
            
            # Hoja 3: Vencidos
            vencidos = self.get_expired_emos()
            if not vencidos.empty:
                export_data["Vencidos"] = vencidos
            
            # Hoja 4: Reporte por área
            reporte_area = self.generate_report_by_area(30)
            if not reporte_area.empty:
                export_data["Reporte_Areas"] = reporte_area
            
            # Hoja 5: Datos completos
            export_data["Datos_Completos"] = self.df
            
            # Exportar usando pandas
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for sheet_name, data in export_data.items():
                    if not data.empty:
                        data.to_excel(writer, sheet_name=sheet_name, index=False)
            
            execution_time = time.time() - start_time
            file_size = filepath.stat().st_size / 1024  # KB
            
            logger.info(f"Excel exportado en {execution_time:.3f}s - "
                       f"Tamaño: {file_size:.1f}KB - Archivo: {filepath.absolute()}")
            
            return str(filepath.absolute())
            
        except Exception as e:
            logger.error(f"Error al exportar Excel: {e}")
            raise
    
    def get_data_quality_report(self) -> Dict:
        """Retorna reporte de calidad de datos."""
        total_records = len(self.df)
        valid_dates = (~self.df["emo_vence"].isna()).sum()
        invalid_dates = self.df["emo_vence"].isna().sum()
        
        return {
            "total_registros": total_records,
            "fechas_validas": valid_dates,
            "fechas_invalidas": invalid_dates,
            "porcentaje_valido": round((valid_dates / total_records) * 100, 2) if total_records > 0 else 0
        }
    
    def get_performance_stats(self) -> Dict:
        """Retorna estadísticas de performance del manager."""
        current_memory = self._get_memory_usage()
        uptime = time.time() - self.performance_stats['creation_time']
        
        return {
            **self.performance_stats,
            "current_memory_mb": round(current_memory, 2),
            "uptime_seconds": round(uptime, 2),
            "cache_stats": {
                "expiring_cache": self.get_expiring_soon.cache_info()._asdict(),
                "expired_cache": self.get_expired_emos.cache_info()._asdict(),
                "priority_cache": self.generate_priority_report.cache_info()._asdict()
            }
        }
    
    def stress_test(self, num_records: int = 5000) -> Dict:
        """
        Ejecuta stress test simplificado.
        """
        logger.info(f"Iniciando stress test con {num_records:,} registros")
        
        # Generar datos de prueba
        import random
        
        start_generation = time.time()
        
        nombres = [f"Empleado_{i:06d}" for i in range(num_records)]
        areas = ["IT", "RRHH", "Logística", "Mantenimiento", "SSOMA"]
        
        # Generar fechas simples
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
        
        generation_time = time.time() - start_generation
        
        # Crear manager y medir performance
        start_processing = time.time()
        initial_memory = self._get_memory_usage()
        
        manager = EMOManagerOptimized(test_data)
        
        creation_time = time.time() - start_processing
        peak_memory = self._get_memory_usage()
        
        # Ejecutar operaciones principales
        start_operations = time.time()
        
        expiring = manager.get_expiring_soon(30)
        expired = manager.get_expired_emos()
        priority_report = manager.generate_priority_report()
        
        operations_time = time.time() - start_operations
        
        # Métricas finales
        results = {
            "records_processed": num_records,
            "data_generation_time": round(generation_time, 3),
            "manager_creation_time": round(creation_time, 3),
            "operations_time": round(operations_time, 3),
            "total_time": round(generation_time + creation_time + operations_time, 3),
            "initial_memory_mb": round(initial_memory, 2),
            "peak_memory_mb": round(peak_memory, 2),
            "memory_increase_mb": round(peak_memory - initial_memory, 2),
            "records_per_second": round(num_records / (creation_time + operations_time), 0),
            "expiring_found": len(expiring),
            "expired_found": len(expired)
        }
        
        logger.info(f"Stress test completado: {results['records_per_second']:,.0f} registros/segundo")
        
        return results

# EJEMPLO DE USO SIMPLIFICADO - SIN EMOJIS
if __name__ == "__main__":
    # Datos de prueba
    empleados_test = {
        "Nombre": ["juan pérez", "MARÍA GONZÁLEZ", "Carlos López", "Lucía Rodríguez", 
                  "Pedro Martín", "Ana Torres"],
        "Area": ["it", "RRHH", "IT", "logística", "Mantenimiento", "rrhh"],
        "emo_vence": ["2025-07-15", "2025-06-25", None, "25/08/2025", 
                     "2024-12-01", "2025-06-30"]
    }
    
    try:
        print("EMO MANAGER OPTIMIZADO - VERSIÓN SIMPLIFICADA")
        print("=" * 60)
        
        # Crear manager optimizado
        start_time = time.time()
        manager = EMOManagerOptimized(empleados_test)
        creation_time = time.time() - start_time
        
        print(f"[OK] Manager creado en {creation_time:.3f}s")
        
        # Performance stats iniciales
        perf_stats = manager.get_performance_stats()
        print(f"[INFO] Memoria inicial: {perf_stats['current_memory_mb']:.2f}MB")
        
        # Ejecutar análisis
        print("\nEJECUTANDO ANÁLISIS...")
        
        # Test de operaciones básicas
        print("Generando reportes...")
        proximos = manager.get_expiring_soon(60)
        vencidos = manager.get_expired_emos()
        prioridades = manager.generate_priority_report()
        
        print(f"[OK] Análisis completado:")
        print(f"  • EMOs próximos (60 días): {len(proximos)}")
        print(f"  • EMOs vencidos: {len(vencidos)}")
        print(f"  • Total registros válidos: {prioridades['total_validos']}")
        
        # Mostrar prioridades
        print(f"\nDISTRIBUCIÓN POR PRIORIDAD:")
        for categoria, cantidad in prioridades.items():
            if categoria != 'total_validos':
                status = "[URGENTE]" if "vencido" in categoria or "urgente" in categoria else "[ALTA]" if "alta" in categoria else "[NORMAL]"
                print(f"  {status} {categoria.replace('_', ' ').title()}: {cantidad}")
        
        # Mostrar algunos detalles
        if len(vencidos) > 0:
            print(f"\nEMOs VENCIDOS:")
            for _, row in vencidos.head(3).iterrows():
                print(f"  [VENCIDO] {row['Nombre']} ({row['Area']}): vencido hace {row['dias_vencido']} días")
        
        if len(proximos) > 0:
            print(f"\nPRÓXIMOS A VENCER:")
            for _, row in proximos.head(3).iterrows():
                print(f"  [PRÓXIMO] {row['Nombre']} ({row['Area']}): vence en {row['dias_restantes']} días")
        
        # Test de cache
        print(f"\nPROBANDO CACHE...")
        start = time.time()
        proximos_cached = manager.get_expiring_soon(60)
        cached_time = time.time() - start
        print(f"[OK] Cache funcionando: consulta en {cached_time:.4f}s")
        
        # Exportar a Excel
        print(f"\nEXPORTANDO REPORTE...")
        try:
            archivo_excel = manager.export_to_excel("reporte_emo_simplificado.xlsx")
            print(f"[OK] Reporte exportado: {archivo_excel}")
        except Exception as e:
            print(f"[ERROR] Error en exportación Excel: {e}")
        
        # Reporte por área
        reporte_area = manager.generate_report_by_area(60)
        if not reporte_area.empty:
            print(f"\nREPORTE POR ÁREA (60 días):")
            for _, row in reporte_area.iterrows():
                print(f"  [ÁREA] {row['Area']}: {row['Cantidad']} empleados (promedio: {row['Promedio_Dias']} días)")
        
        # Calidad de datos
        calidad = manager.get_data_quality_report()
        print(f"\nCALIDAD DE DATOS:")
        for metrica, valor in calidad.items():
            print(f"  • {metrica.replace('_', ' ').title()}: {valor}")
        
        # Mini stress test
        print(f"\nMINI STRESS TEST...")
        stress_results = manager.stress_test(1000)
        print(f"[OK] Procesados: {stress_results['records_processed']:,} registros")
        print(f"[SPEED] Velocidad: {stress_results['records_per_second']:,.0f} registros/segundo")
        print(f"[MEMORY] Memoria usada: +{stress_results['memory_increase_mb']:.1f}MB")
        
        print(f"\n[SUCCESS] EMO MANAGER FUNCIONANDO PERFECTAMENTE!")
        print(f"[INFO] Compatible con Windows - Sin errores de timezone/emojis")
        
    except Exception as e:
        logger.error(f"Error en ejecución: {e}")
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()