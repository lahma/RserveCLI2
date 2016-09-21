FROM microsoft/dotnet:latest

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF

RUN echo "deb http://download.mono-project.com/repo/debian wheezy-libjpeg62-compat main" > /etc/apt/sources.list.d/mono-xamarin.list \
  && echo "deb http://download.mono-project.com/repo/debian wheezy main" >> /etc/apt/sources.list.d/mono-xamarin.list \
  && apt-get update \
  && apt-get install -y binutils mono-devel nuget referenceassemblies-pcl \
  && rm -rf /var/lib/apt/lists/* /tmp/*

RUN mkdir /app
ADD RServeCLI2 app/RServeCLI2
ADD RServeCLI2.Example app/RServeCLI2.Example
ADD RServeCLI2.Test app/RServeCLI2.Test

WORKDIR /app
RUN dotnet restore

WORKDIR /app/RServeCLI2
RUN dotnet build -f netstandard1.3

WORKDIR /app/RServeCLI2.Example
RUN dotnet build -f netcoreapp1.0

WORKDIR /app/RServeCLI2.Test
RUN dotnet test -f netcoreapp1.0

