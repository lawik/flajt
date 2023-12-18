FROM python:3.8-slim as base
RUN pip install --no-cache-dir pyarrow

FROM base
RUN pip install --no-cache-dir adbc-driver-sqlite
