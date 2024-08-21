INSTALL_DIR=/opt/

# Spark-3.2
cd ${INSTALL_DIR} && \
wget https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz && \
tar --strip-components=1 -xf spark-3.2.2-bin-hadoop3.2.tgz spark-3.2.2-bin-hadoop3.2/jars/ && \
rm -rf spark-3.2.2-bin-hadoop3.2.tgz && \
mkdir -p ${INSTALL_DIR}//shims/spark32/spark_home/assembly/target/scala-2.12 && \
mv jars ${INSTALL_DIR}//shims/spark32/spark_home/assembly/target/scala-2.12 && \
wget https://github.com/apache/spark/archive/refs/tags/v3.2.2.tar.gz && \
tar --strip-components=1 -xf v3.2.2.tar.gz spark-3.2.2/sql/core/src/test/resources/  && \
mkdir -p shims/spark32/spark_home/ && \
mv sql shims/spark32/spark_home/

# Spark-3.3
cd ${INSTALL_DIR} && \
wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && \
tar --strip-components=1 -xf spark-3.3.1-bin-hadoop3.tgz spark-3.3.1-bin-hadoop3/jars/ && \
rm -rf spark-3.3.1-bin-hadoop3.tgz && \
mkdir -p ${INSTALL_DIR}//shims/spark33/spark_home/assembly/target/scala-2.12 && \
mv jars ${INSTALL_DIR}//shims/spark33/spark_home/assembly/target/scala-2.12 && \
wget https://github.com/apache/spark/archive/refs/tags/v3.3.1.tar.gz && \
tar --strip-components=1 -xf v3.3.1.tar.gz spark-3.3.1/sql/core/src/test/resources/  && \
mkdir -p shims/spark33/spark_home/ && \
mv sql shims/spark33/spark_home/

# Spark-3.4
cd ${INSTALL_DIR} && \
wget https://archive.apache.org/dist/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz && \
tar --strip-components=1 -xf spark-3.4.2-bin-hadoop3.tgz spark-3.4.2-bin-hadoop3/jars/ && \
rm -rf spark-3.4.2-bin-hadoop3.tgz && \
mkdir -p ${INSTALL_DIR}//shims/spark34/spark_home/assembly/target/scala-2.12 && \
mv jars ${INSTALL_DIR}//shims/spark34/spark_home/assembly/target/scala-2.12 && \
wget https://github.com/apache/spark/archive/refs/tags/v3.4.2.tar.gz && \
tar --strip-components=1 -xf v3.4.2.tar.gz spark-3.4.2/sql/core/src/test/resources/  && \
mkdir -p shims/spark34/spark_home/ && \
mv sql shims/spark34/spark_home/

# Spark-3.5
cd ${INSTALL_DIR} && \
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
tar --strip-components=1 -xf spark-3.5.1-bin-hadoop3.tgz spark-3.5.1-bin-hadoop3/jars/ && \
rm -rf spark-3.5.1-bin-hadoop3.tgz && \
mkdir -p ${INSTALL_DIR}//shims/spark35/spark_home/assembly/target/scala-2.12 && \
mv jars ${INSTALL_DIR}//shims/spark35/spark_home/assembly/target/scala-2.12 && \
wget https://github.com/apache/spark/archive/refs/tags/v3.5.1.tar.gz && \
tar --strip-components=1 -xf v3.5.1.tar.gz spark-3.5.1/sql/core/src/test/resources/  && \
mkdir -p shims/spark35/spark_home/ && \
mv sql shims/spark35/spark_home/