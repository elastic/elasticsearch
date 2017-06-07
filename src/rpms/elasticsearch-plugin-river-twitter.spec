%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch-plugin-river-twitter
Version:        1.1.0
Release:        1%{?dist}
Summary:        ElasticSearch plugin to hook into Twitter.

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            https://github.com/elasticsearch/elasticsearch-river-twitter

Source0:        https://github.com/downloads/elasticsearch/elasticsearch-river-twitter/elasticsearch-river-twitter-1.1.0.zip
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       elasticsearch >= 0.19

%description
The Twitter River plugin allows index twitter stream.

%prep
rm -fR %{name}-%{version}
%{__mkdir} -p %{name}-%{version}
cd %{name}-%{version}
%{__mkdir} -p plugins
unzip %{SOURCE0} -d plugins/river-twitter

%build
true

%install
rm -rf $RPM_BUILD_ROOT
cd %{name}-%{version}
%{__mkdir} -p %{buildroot}/opt/elasticsearch/plugins
%{__install} -D -m 755 plugins/river-twitter/elasticsearch-river-twitter-%{version}.jar %{buildroot}/opt/elasticsearch/plugins/river-twitter/elasticsearch-river-twitter.jar
%{__install} -D -m 755 plugins/river-twitter/twitter4j-stream-2.2.5.jar -t %{buildroot}/opt/elasticsearch/plugins/river-twitter/
%{__install} -D -m 755 plugins/river-twitter/twitter4j-core-2.2.5.jar -t %{buildroot}/opt/elasticsearch/plugins/river-twitter/

%files
%defattr(-,root,root,-)
%dir /opt/elasticsearch/plugins/river-twitter
/opt/elasticsearch/plugins/river-twitter/*

%changelog
* Tue Feb 22 2012 Sean Laurent
- Initial package

