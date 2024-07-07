from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.routers import endpoints
import uvicorn


app = FastAPI()



app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

# include routers

app.include_router(endpoints.router)

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000, reload=True)

