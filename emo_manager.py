import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EMOManager:
    """Gestiona análisis de vencimientos de EMO con manejo robusto de errores."""
    
    def __init__(self, empleados_data: Dict):
        """
        Inicializa el manager con datos de empleados.
        
        Args:
            empleados_data: Dict con keys 'Nombre', 'Area', 'emo_vence'
        """
        if not empleados_data:
            raise ValueError("Los datos de empleados no pueden estar vacíos")
            
        self.df = self._create_dataframe(empleados_data)
        self._clean_data()
    
    def _create_dataframe(self, data: Dict) -> pd.DataFrame:
        """Crea DataFrame y valida estructura básica."""
        required_columns = {'Nombre', 'Area', 'emo_vence'}
        
        if not required_columns.issubset(data.keys()):
            missing = required_columns - data.keys()
            raise ValueError(f"Faltan columnas requeridas: {missing}")
        
        df = pd.DataFrame(data)
        logger.info(f"DataFrame creado con {len(df)} registros")
        return df
    
    def _clean_data(self):
        """Limpia y normaliza los datos."""
        initial_count = len(self.df)
        
        # Eliminar duplicados
        self.df = self.df.drop_duplicates()
        logger.info(f"Duplicados eliminados: {initial_count - len(self.df)}")
        
        # Normalizar nombres
        self.df["Nombre"] = self.df["Nombre"].str.strip().str.upper()
        
        # Convertir fechas con manejo de errores
        self.df["emo_vence"] = pd.to_datetime(
            self.df["emo_vence"], 
            errors="coerce", 
            dayfirst=True
        )
        
        # Contar errores de conversión
        invalid_dates = self.df["emo_vence"].isna().sum()
        if invalid_dates > 0:
            logger.warning(f"Fechas inválidas encontradas: {invalid_dates}")
    
    def get_expiring_soon(self, days_ahead: int = 30) -> pd.DataFrame:
        """
        Obtiene EMOs que vencen en los próximos N días.
        
        Args:
            days_ahead: Número de días hacia adelante a verificar
            
        Returns:
            DataFrame con EMOs próximos a vencer
        """
        if days_ahead <= 0:
            raise ValueError("days_ahead debe ser mayor a 0")
        
        hoy = datetime.today()
        fecha_limite = hoy + timedelta(days=days_ahead)
        
        # Filtrar solo fechas válidas
        df_validas = self.df.dropna(subset=["emo_vence"])
        
        # Filtrar rango de fechas
        mask = (df_validas["emo_vence"] >= hoy) & (df_validas["emo_vence"] <= fecha_limite)
        df_expiring = df_validas[mask]
        
        logger.info(f"EMOs próximos a vencer en {days_ahead} días: {len(df_expiring)}")
        return df_expiring
    
    def generate_report_by_area(self, days_ahead: int = 30) -> pd.DataFrame:
        """
        Genera reporte agrupado por área.
        
        Args:
            days_ahead: Días hacia adelante para el análisis
            
        Returns:
            DataFrame con reporte por área
        """
        df_expiring = self.get_expiring_soon(days_ahead)
        
        if df_expiring.empty:
            logger.info("No hay EMOs próximos a vencer")
            return pd.DataFrame(columns=["Area", "Empleados", "Cantidad"])
        
        # Método más eficiente que apply(list)
        reporte = (df_expiring.groupby("Area", as_index=False)
                  .agg({
                      "Nombre": ["count", lambda x: x.tolist()]
                  }))
        
        # Aplanar columnas multi-level
        reporte.columns = ["Area", "Cantidad", "Empleados"]
        
        return reporte
    
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

# EJEMPLO DE USO Y TESTING
if __name__ == "__main__":
    # Datos de prueba
    empleados_test = {
        "Nombre": ["Juan", "María", "Carlos", "Lucía", "Pedro", "Juan"],  # Duplicado
        "Area": ["IT", "RRHH", "IT", "Logística", "Mantenimiento", "IT"],
        "emo_vence": ["2025-07-15", "2025-06-25", None, "25/08/2025", "Fecha incorrecta", "2025-07-15"]
    }
    
    try:
        # Crear manager
        emo_manager = EMOManager(empleados_test)
        
        # Generar reporte
        reporte = emo_manager.generate_report_by_area(days_ahead=60)
        print("📊 REPORTE POR ÁREA:")
        print(reporte)
        
        # Reporte de calidad
        quality = emo_manager.get_data_quality_report()
        print(f"\n📈 CALIDAD DE DATOS:")
        for key, value in quality.items():
            print(f"  {key}: {value}")
            
        # Errores específicos
        errores = emo_manager.df[emo_manager.df["emo_vence"].isna()]
        if not errores.empty:
            print("\n⚠️ REGISTROS CON ERRORES:")
            print(errores[["Nombre", "Area"]])
            
    except Exception as e:
        logger.error(f"Error en ejecución: {e}")
