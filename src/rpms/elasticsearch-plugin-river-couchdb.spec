%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-river-couchdb
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to hook into CouchDB.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-river-couchdb

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-river-couchdb/elasticsearch-river-couchdb-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The CouchDB River plugin allows to hook into couchdb _changes feed and automatically index it into elasticsearch.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/river-couchdb

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/river-couchdb/elasticsearch-river-couchdb-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/river-couchdb/elasticsearch-river-couchdb.jar

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/river-couchdb
/opt/elasticsearch/plugins/river-couchdb/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

