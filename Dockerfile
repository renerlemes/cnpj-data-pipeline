FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project files
COPY pyproject.toml .
COPY config.py database.py downloader.py processor.py parquet_writer.py main.py ./
COPY initial.sql /app/initial.sql

# Install dependencies
RUN uv pip install --system -e .

ENV TEMP_DIR=/app/temp
# Standalone image: write Parquet without a database. Override to postgres + DATABASE_URL when needed.
ENV PARQUET_OUTPUT_DIR=/app/parquet
ENV OUTPUT_FORMAT=parquet
RUN mkdir -p /app/temp /app/parquet

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import main" || exit 1

ENTRYPOINT ["cnpj-pipeline"]
