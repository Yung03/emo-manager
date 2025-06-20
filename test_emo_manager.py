import pytest
from datetime import datetime
from emo_manager import EMOManager  # Ajusta el nombre si tu archivo se llama distinto

@pytest.fixture
def sample_data():
    return {
        "Nombre": ["Ana", "Luis", "Marta"],
        "Area": ["SSOMA", "Logística", "SSOMA"],
        "emo_vence": ["2025-06-30", "30/07/2025", None]
    }

def test_creacion_manager(sample_data):
    manager = EMOManager(sample_data)
    assert len(manager.df) == 3
    assert "Nombre" in manager.df.columns

def test_get_expiring_soon(sample_data):
    manager = EMOManager(sample_data)
    resultado = manager.get_expiring_soon(90)
    assert isinstance(resultado, type(manager.df))
    assert resultado.shape[0] >= 0

def test_data_quality_report(sample_data):
    manager = EMOManager(sample_data)
    report = manager.get_data_quality_report()
    assert report["total_registros"] == 3
    assert "fechas_invalidas" in report
