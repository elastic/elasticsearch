%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-river-rabbitmq
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to hook into RabbitMQ.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-river-rabbitmq

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-river-rabbitmq/elasticsearch-river-rabbitmq-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The RabbitMQ River plugin allows index bulk format messages into elasticsearch.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/river-rabbitmq

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/river-rabbitmq/elasticsearch-river-rabbitmq-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/river-rabbitmq/elasticsearch-river-rabbitmq.jar
%{__install} -D -m 755 plugins/river-rabbitmq/amqp-client-2.7.0.jar -t %{buildroot}/opt/elasticsearch/plugins/river-rabbitmq/
%{__install} -D -m 755 plugins/river-rabbitmq/commons-cli-1.1.jar -t %{buildroot}/opt/elasticsearch/plugins/river-rabbitmq/
%{__install} -D -m 755 plugins/river-rabbitmq/commons-io-1.2.jar -t %{buildroot}/opt/elasticsearch/plugins/river-rabbitmq/

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/river-rabbitmq
/opt/elasticsearch/plugins/river-rabbitmq/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

