%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-analysis-smartcn
Version:        1.2.0
Release:        1%{?dist}
Summary:        Integrates Lucene Smart Chinese analysis module into elasticsearch.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-analysis-smartcn

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-analysis-smartc/elasticsearch-analysis-smartc-%{version}.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The Smart Chinese Analysis plugin integrates Lucene Smart Chinese analysis module into elasticsearch.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/analysis-smartcn

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/analysis-smartcn/elasticsearch-analysis-smartcn-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/analysis-smartcn/elasticsearch-analysis-smartcn.jar
%{__install} -m 755 plugins/analysis-smartcn/lucene-smartcn-*.jar -t %{buildroot}/opt/elasticsearch/plugins/analysis-smartcn

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/analysis-smartcn
/opt/elasticsearch/plugins/analysis-smartcn/*

%changelog
* Tue Apr 10 2012 Sean Laurent
- Initial package
