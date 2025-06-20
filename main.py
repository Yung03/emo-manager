from emo_manager import EMOManager

# Datos de prueba
empleados_test = {
    "Nombre": ["Juan", "María", "Carlos", "Lucía", "Pedro", "Juan"],  # Duplicado
    "Area": ["IT", "RRHH", "IT", "Logística", "Mantenimiento", "IT"],
    "emo_vence": ["2025-07-15", "2025-06-25", None, "25/08/2025", "Fecha incorrecta", "2025-07-15"]
}

if __name__ == "__main__":
    manager = EMOManager(empleados_test)
    reporte = manager.generate_report_by_area(60)
    manager.export_to_excel(reporte, "reporte_area.xlsx")

    print("\n📊 REPORTE POR ÁREA:")
    print(reporte)

    print("\n📈 CALIDAD DE DATOS:")
    print(manager.get_data_quality_report())
