%define debug_package %{nil}

# Avoid running brp-java-repack-jars
%define __os_install_post %{nil}

Name:           elasticsearch
Version:        0.19.1
Release:        1%{?dist}
Summary:        A distributed, highly available, RESTful search engine

Group:          System Environment/Daemons
License:        ASL 2.0
URL:            http://www.elasticsearch.com
Source0:        https://github.com/downloads/%{name}/%{name}/%{name}-%{version}.tar.gz
Source1:        elasticsearch-init.d
Source2:        elasticsearch-logrotate.d
Source3:        elasticsearch-config-logging.yml
Source4:        elasticsearch-sysconfig
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       jpackage-utils
Requires:       java

Requires(post): chkconfig initscripts
Requires(pre):  chkconfig initscripts
Requires(pre):  shadow-utils

%description
A distributed, highly available, RESTful search engine

%prep
%setup -q -n %{name}-%{version}

%build
true

%install
rm -rf $RPM_BUILD_ROOT

%{__mkdir} -p %{buildroot}/opt/%{name}/bin
%{__install} -p -m 755 bin/elasticsearch %{buildroot}/opt/%{name}/bin
%{__install} -p -m 644 bin/elasticsearch.in.sh %{buildroot}/opt/%{name}/bin
%{__install} -p -m 755 bin/plugin %{buildroot}/opt/%{name}/bin

#libs
%{__mkdir} -p %{buildroot}/opt/%{name}/lib/sigar
%{__install} -p -m 644 lib/*.jar %{buildroot}/opt/%{name}/lib
%{__install} -p -m 644 lib/sigar/*.jar %{buildroot}/opt/%{name}/lib/sigar
%ifarch i386
%{__install} -p -m 644 lib/sigar/libsigar-x86-linux.so %{buildroot}/opt/%{name}/lib/sigar
%endif
%ifarch x86_64
%{__install} -p -m 644 lib/sigar/libsigar-amd64-linux.so %{buildroot}/opt/%{name}/lib/sigar
%endif

# config
%{__mkdir} -p %{buildroot}/opt/%{name}/config
%{__install} -m 644 config/elasticsearch.yml %{buildroot}/opt/%{name}/config
%{__install} -m 644 %{SOURCE3} %{buildroot}/opt/%{name}/config/logging.yml

# data
%{__mkdir} -p %{buildroot}%{_localstatedir}/lib/%{name}

# logs
%{__mkdir} -p %{buildroot}%{_localstatedir}/log/%{name}
%{__install} -D -m 644 %{SOURCE2} %{buildroot}%{_sysconfdir}/logrotate.d/elasticsearch

# plugins
%{__mkdir} -p %{buildroot}/opt/%{name}/plugins

# sysconfig and init
%{__mkdir} -p %{buildroot}%{_sysconfdir}/rc.d/init.d
%{__mkdir} -p %{buildroot}%{_sysconfdir}/sysconfig
%{__install} -m 755 %{SOURCE1} %{buildroot}%{_sysconfdir}/rc.d/init.d/elasticsearch
%{__install} -m 755 %{SOURCE4} %{buildroot}%{_sysconfdir}/sysconfig/elasticsearch

%{__mkdir} -p %{buildroot}%{_localstatedir}/run/elasticsearch
%{__mkdir} -p %{buildroot}%{_localstatedir}/lock/subsys/elasticsearch

%pre
# create elasticsearch group
if ! getent group elasticsearch >/dev/null; then
        groupadd -r elasticsearch
fi

# create elasticsearch user
if ! getent passwd elasticsearch >/dev/null; then
        useradd -r -g elasticsearch -d /opt/%{name} \
            -s /sbin/nologin -c "You know, for search" elasticsearch
fi

%post
/sbin/chkconfig --add elasticsearch

%preun
if [ $1 -eq 0 ]; then
  /sbin/service elasticsearch stop >/dev/null 2>&1
  /sbin/chkconfig --del elasticsearch
fi

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_sysconfdir}/rc.d/init.d/elasticsearch
%config(noreplace) %{_sysconfdir}/sysconfig/elasticsearch
%{_sysconfdir}/logrotate.d/elasticsearch
%dir /opt/elasticsearch
/opt/elasticsearch/bin/*
/opt/elasticsearch/config/*
/opt/elasticsearch/lib/*
%dir /opt/elasticsearch/plugins
%doc LICENSE.txt  NOTICE.txt  README.textile
%defattr(-,elasticsearch,elasticsearch,-)
%dir %{_localstatedir}/lib/elasticsearch
%{_localstatedir}/run/elasticsearch
%dir %{_localstatedir}/log/elasticsearch

%changelog
* Tue Apr 10 2012 Sean Laurent - 0.19.1
- New upstream version.

* Thu Feb 22 2012 Sean Laurent - 0.19.RC2
- New upstream version.

* Thu Dec 28 2011 Tavis Aitken <tavisto@tavisto.net> - 0.18.6-1
- New upstream version

* Thu Dec 01 2011 Tavis Aitken <tavisto@tavisto.net> - 0.18.5-1
- New upstream version

* Sat Nov 26 2011 Tavis Aitken <tavisto@tavisto.net> - 0.18.4-1
- New upstream version

* Mon Oct 31 2011 Dan Carley <dan.carley@gmail.com> - 0.18.2-2
- Raise open files limit.

* Sat Oct 29 2011 Tavis Aitken <tavisto@tavisto.net> - 0.18.2-1
- New upstream version.

* Mon Jul 25 2011 Erick Tryzelaar erick.tryzelaar@gmail.com 0.17.1-1
- New Upstream version 

* Thu Jul 14 2011 Erick Tryzelaar erick.tryzelaar@gmail.com 0.16.4-1
- New Upstream version 
- Add analysis-icu, cloud-aws, hadoop, lang-groovy, lang-python,
  mapper-attachments, transport-memcached, transport-thrift, and
  transports-wares plugin subpackages

* Fri Jun 24 2011 Dan Everton dan.everton@wotifgroup.com 0.16.2-2
- Set plugins path so they are automatically detected by ES.

* Thu Jun 02 2011 Dan Everton dan.everton@wotifgroup.com 0.16.2-1
- New Upstream version 

* Wed May 11 2011 Tavis Aitken tavisto@tavisto.net 0.16.0-2
- Add lang-javascript plugin subpackage

* Thu Apr 28 2011 Tavis Aitken tavisto@tavisto.net 0.16.0-1
- New upstream version 

* Wed Apr 06 2011 Dan Carley <dan.carley@gmail.com> 0.15.2-2
- Moved data to /var/lib
- Allow customisation of paths.
- Allow customisation of memory and include settings.

* Mon Mar 07 2011 Tavis Aitken tavisto@tavisto.net 0.15.2-1
- New Upstream version 

* Mon Mar 07 2011 Tavis Aitken tavisto@tavisto.net 0.15.1-1
- New Upstream version 

* Mon Mar 01 2011 Tavis Aitken tavisto@tavisto.net 0.15.0-1
- New Upstream version 

* Mon Feb 28 2011 Tavis Aitken tavisto@tavisto.net 0.14.3-0
- New Upstream version 

* Sat Jan 29 2011 Tavis Aitken tavisto@tavisto.net 0.14.3-2
- Fixed the paths for the plugin sub-packages

* Sat Jan 29 2011 Tavis Aitken tavisto@tavisto.net 0.14.3-1
- Update to upstream version, complete with river plugin sub-packages

* Sat Jan 29 2011 Tavis Aitken tavisto@tavisto.net 0.14.2-3
- Fixed the user creation comment to not include a colon

* Fri Jan 21 2011  Tavis Aitken <tavisto@tavisto.net> - 0.14.2-2
- Fixed the logging.yml and logrotate.d configs

* Fri Jan 14 2011 Tavis Aitken <tavisto@tavisto.net> - 0.14.2-1
- New upstream version, and added specific arch suport for the sigar libraries.

* Tue Jan 4 2011 Tavis Aitken <tavisto@tavisto.net> - 0.14.1-1
- Initial package
