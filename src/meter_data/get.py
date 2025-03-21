
from psycopg.rows import dict_row

from ..models import Response
from ..utils import db_connection

from fastapi import APIRouter
import logging


get_meter_data_router = APIRouter()

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("get-meter-data")

@get_meter_data_router.get("/meter_data/{meter_data_id}")
async def get_meter_data_by_id(meter_data_id : str) -> Response:
    with db_connection() as conn:
        with conn.cursor(row_factory = dict_row) as cursor:
            cursor.execute("SELECT * FROM meter_data WHERE meter_data_id = %s", (meter_data_id,))
            data = cursor.fetchall()

            if not data:
                logger.warning("No data found")
                return Response(status_code=404, message="Not found error")
            logger.info("Data successfully retrieved")
            return Response(status_code=200, message={"meter_data": data})

@get_meter_data_router.get("/meter_data/{connection_ean_code}")
async def get_meter_data_by_conn_ean_code(connection_ean_code : str) -> Response:
    with db_connection() as conn:
        with conn.cursor(row_factory = dict_row) as cursor:
            cursor.execute('SELECT * FROM meter_data WHERE connection_ean_code = %s', (connection_ean_code,))
            data = cursor.fetchall()

            if not data:
                logger.warning("No data found")
                return Response(status_code=404, message="Not found error")
            logger.info("Data successfully retrieved")
            return Response(status_code=200, message={"meter_data": data})

@get_meter_data_router.get("/meter_data/{business_partner_id}")
async def get_meter_data_by_business_partner_id(business_partner_id : str) -> Response:
    with db_connection() as conn:
        with conn.cursor(row_factory = dict_row) as cursor:
            cursor.execute('SELECT * FROM meter_data WHERE business_partner_id = %s', (business_partner_id,))
            data = cursor.fetchall()

            if not data:
                logger.warning("No data found for ")
                return Response(status_code=404, message="Not found error")
            logger.info("Data successfully retrieved")
            return Response(status_code=200, message={"meter_data": data})

@get_meter_data_router.get("/meter_data/")
async def get_meter_data_by_params(connection_ean_code: str, business_partner_id: str) -> Response:
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            query = """
                SELECT * FROM meter_data 
                WHERE connection_ean_code = %s AND business_partner_id = %s
            """
            cursor.execute(query, (connection_ean_code, business_partner_id))
            data = cursor.fetchall()

            if not data:
                logger.warning("No data found for connection_ean_code: %s and business_partner_id: %s", connection_ean_code, business_partner_id)
                return Response(status_code=404, message="Not found error")

            logger.info("Data successfully retrieved for connection_ean_code: %s and business_partner_id: %s", connection_ean_code, business_partner_id)
            return Response(status_code=200, message={"meter_data": data})