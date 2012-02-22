%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-lang-groovy
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to use Groovy for script execution.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-lang-groovy

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-lang-groovy/elasticsearch-lang-groovy-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The Groovy language plugin allows to have groovy as the language of scripts to execute.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/lang-groovy

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/lang-groovy/elasticsearch-lang-groovy-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/lang-groovy/elasticsearch-lang-groovy.jar
%{__install} -D -m 755 plugins/lang-groovy/groovy-all-1.8.4.jar -t %{buildroot}/opt/elasticsearch/plugins/lang-groovy/

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/lang-groovy
/opt/elasticsearch/plugins/lang-groovy/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

