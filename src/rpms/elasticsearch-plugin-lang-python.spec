%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-lang-python
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to use Python for script execution.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-lang-python

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-lang-python/elasticsearch-lang-python-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The Groovy language plugin allows to have Python as the language of scripts to execute.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/lang-python

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/lang-python/elasticsearch-lang-python-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/lang-python/elasticsearch-lang-python.jar
%{__install} -D -m 755 plugins/lang-python/jython-standalone-2.5.2.jar -t %{buildroot}/opt/elasticsearch/plugins/lang-python/

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/lang-python
/opt/elasticsearch/plugins/lang-python/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

