from psycopg.rows import dict_row

from src.models import Response
from ..utils import db_connection

from fastapi import APIRouter

get_meter_readings_router = APIRouter()

@get_meter_readings_router.get("/meter_readings/{meter_readings_id}")
async def get_meter_readings_by_id(meter_readings_id : str) -> Response:
    conn = db_connection()
    cursor = conn.cursor(row_factory = dict_row)

    cursor.execute("SELECT * FROM meter_readings WHERE meter_readings_id = %s", (meter_readings_id,))
    data = cursor.fetchone()

    cursor.close()
    conn.close()

    if data:
        return Response(status_code=200, message={"meter_readings":data})
    else:
        return Response(status_code=404, message="Not found error")