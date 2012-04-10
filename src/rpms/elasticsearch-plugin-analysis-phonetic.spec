%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-analysis-phonetic
Version:        1.2.0
Release:        1%{?dist}
Summary:        Integrates phonetic token filter analysis with elasticsearch.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-analysis-phonetic

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-analysis-phonetic/elasticsearch-analysis-phonetic-%{version}.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The Phonetic Analysis plugin integrates phonetic token filter analysis with elasticsearch.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/analysis-phonetic

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/analysis-phonetic/elasticsearch-analysis-phonetic-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/analysis-phonetic/elasticsearch-analysis-phonetic.jar
%{__install} -m 755 plugins/analysis-phonetic/commons-codec-*.jar -t %{buildroot}/opt/elasticsearch/plugins/analysis-phonetic

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/analysis-phonetic
/opt/elasticsearch/plugins/analysis-phonetic/*

%changelog
* Tue Apr 10 2012 Sean Laurent
- Initial package
