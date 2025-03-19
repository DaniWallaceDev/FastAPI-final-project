from psycopg.rows import dict_row

from ..models import Response
from ..utils import db_connection

from fastapi import APIRouter

get_mandate_data_router = APIRouter()

@get_mandate_data_router.get("/mandate_data/{mandate_id}")
async def get_mandate_data_by_id(mandate_id : str) -> Response:
    conn = db_connection()
    cursor = conn.cursor(row_factory = dict_row)

    cursor.execute("SELECT * FROM mandate_data WHERE mandate_id = %s", (mandate_id,))
    data = cursor.fetchone()

    cursor.close()
    conn.close()

    if data:
        return Response(status_code=200, message={"mandate_data":data})
    else:
        return Response(status_code=404, message="Not found error")
