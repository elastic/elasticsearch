%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-river-wikipedia
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to index Wikipedia.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-river-wikipedia

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-river-wikipedia/elasticsearch-river-wikipedia-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The Wikipedia River plugin allows index Wikipedia.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/river-wikipedia

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/river-wikipedia/elasticsearch-river-wikipedia-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/river-wikipedia/elasticsearch-river-wikipedia.jar

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/river-wikipedia
/opt/elasticsearch/plugins/river-wikipedia/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

