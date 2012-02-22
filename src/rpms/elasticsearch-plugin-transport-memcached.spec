%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-transport-memcached
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to use the REST interface over memcached.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-transport-memcached

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-transport-memcached/elasticsearch-transport-memcached-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The memcached transport plugin allows to use the REST interface over memcached (though with limitations).

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/transport-memcached

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/transport-memcached/elasticsearch-transport-memcached-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/transport-memcached/elasticsearch-transport-memcached.jar

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/transport-memcached
/opt/elasticsearch/plugins/transport-memcached/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

