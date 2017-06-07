%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-transport-thrift
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to use the REST interface over thrift.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-transport-memcached

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-transport-thrift/elasticsearch-transport-thrift-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The memcached transport plugin allows to use the REST interface over thrift on top of HTTP.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/transport-thrift

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/transport-thrift/elasticsearch-transport-thrift-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/transport-thrift/elasticsearch-transport-thrift.jar
%{__install} -D -m 755 plugins/transport-thrift/libthrift-0.6.1.jar -t %{buildroot}/opt/elasticsearch/plugins/transport-thrift/
%{__install} -D -m 755 plugins/transport-thrift/slf4j-api-1.5.8.jar -t %{buildroot}/opt/elasticsearch/plugins/transport-thrift/
%{__install} -D -m 755 plugins/transport-thrift/slf4j-log4j12-1.5.8.jar -t %{buildroot}/opt/elasticsearch/plugins/transport-thrift/
%{__install} -D -m 755 plugins/transport-thrift/commons-lang-2.5.jar -t %{buildroot}/opt/elasticsearch/plugins/transport-thrift/

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/transport-thrift
/opt/elasticsearch/plugins/transport-thrift/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

