from pydantic import BaseModel


class Response(BaseModel):
    status_code: int
    message: dict | str
