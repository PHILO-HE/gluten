FROM centos:7

RUN sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-* || true
RUN sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-* || true

RUN yum install -y centos-release-scl
RUN rm /etc/yum.repos.d/CentOS-SCLo-scl.repo -f
RUN sed -i \
       -e 's/^mirrorlist/#mirrorlist/' \
       -e 's/^#baseurl/baseurl/' \
       -e 's/mirror\.centos\.org/vault.centos.org/' \
       /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

RUN yum install -y git patch wget sudo java-1.8.0-openjdk-devel

RUN git clone --depth=1 https://github.com/apache/incubator-gluten /opt/gluten

RUN echo "check_certificate = off" >> ~/.wgetrc

RUN cd /opt/gluten && bash ./dev/vcpkg/setup-build-depends.sh

# An actual path used for vcpkg cache.
RUN mkdir -p /var/cache/vcpkg

# Set vcpkg cache path.
ENV VCPKG_BINARY_SOURCES=clear;files,/var/cache/vcpkg,readwrite

# Build arrow, then install the native libs to system paths and jar package to .m2/ directory.
RUN cd /opt/gluten && source ./dev/vcpkg/env.sh && bash ./dev/builddeps-veloxbe.sh build_arrow && \
    rm -rf ep/_ep/ && rm -rf /tmp/velox-deps/
