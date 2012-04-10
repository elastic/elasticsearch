%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-analysis-stempel
Version:        1.2.0
Release:        1%{?dist}
Summary:        Integrates Lucene stempel (polish) analysis module into elasticsearch.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-analysis-stempel

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-analysis-stempel/elasticsearch-analysis-stempel-%{version}.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The Stempel (Polish) Analysis plugin integrates Lucene stempel (polish) analysis module into elasticsearch.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/analysis-stempel

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/analysis-stempel/elasticsearch-analysis-stempel-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/analysis-stempel/elasticsearch-analysis-stempel.jar
%{__install} -m 755 plugins/analysis-stempel/lucene-stempel-*.jar -t %{buildroot}/opt/elasticsearch/plugins/analysis-stempel

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/analysis-stempel
/opt/elasticsearch/plugins/analysis-stempel/*

%changelog
* Tue Apr 10 2012 Sean Laurent
- Initial package
