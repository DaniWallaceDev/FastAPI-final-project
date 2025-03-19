
from psycopg.rows import dict_row

from ..models import Response
from ..utils import db_connection

from fastapi import APIRouter

get_meter_data_router = APIRouter()

@get_meter_data_router.get("/meter_data/{meter_data_id}")
async def get_meter_data_by_id(meter_data_id : str) -> Response:
    conn = db_connection()
    cursor = conn.cursor(row_factory = dict_row)

    cursor.execute("SELECT * FROM meter_data WHERE meter_data_id = %s", (meter_data_id,))
    data = cursor.fetchone()

    cursor.close()
    conn.close()

    if data:
        return Response(status_code=200, message={"meter_data":data})
    else:
        return Response(status_code=404, message="Not found error")
