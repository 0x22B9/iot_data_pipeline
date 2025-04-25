FROM spark:3.5.5-scala2.12-java17-python3-ubuntu

WORKDIR /app

ARG USER_ID=1001
ARG GROUP_ID=0

USER root

RUN mkdir -p /app && \
    useradd --create-home --no-log-init --uid ${USER_ID} --gid ${GROUP_ID} sparky && \
    chown -R ${USER_ID}:${GROUP_ID} /app

ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

COPY jars /app/jars

USER ${USER_ID}

COPY . .

CMD ["bash"]