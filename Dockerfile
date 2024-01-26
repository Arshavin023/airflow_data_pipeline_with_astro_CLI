FROM quay.io/astronomer/astro-runtime:10.2.0

# install dbt into a venv to avoid package dependency conflicts
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./

# Set up virtual environment and install dbt
RUN python -m virtualenv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && \
    deactivate


