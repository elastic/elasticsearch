---
navigation_title: "Known issues"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-known-issues.html
---

# Elasticsearch known issues [elasticsearch-known-issues]
Known issues are significant defects or limitations that may impact your implementation. These issues are actively being worked on and will be addressed in a future release. Review the Elasticsearch known issues to help you make informed decisions, such as upgrading to a new version.

## 9.0.0 [elasticsearch-9.0.0-known-issues]
* Elasticsearch on Windows may fail to start, or it may forbid some file-related operations, when referencing paths with a case different from the one stored by the filesystem. Windows paths are treated in a case-sensitive way, but the filesystem stores them with case. _Entitlements_, the new security system used by Elasticsearch, treat all paths as case-sensitive, and can therefore prevent access to a path that should be accessible. An example: if Elasticsearch is installed in  `C:\ELK\elasticsearch`, and you try to launch it as `c:\elk\elasticsearch\bin\elasticsearch.bat`, you will get a `NotEntitledException` while booting. We block access to `c:\elk\elasticsearch` as it does not match with `C:\ELK\elasticsearch`.\
As a workaround, please ensure that all paths you specify (command line, config files, etc.) have the same casing as stored in the filesystem (i.e. the exact same files and directory names as showed in Windows Explorer on in `cmd`).
* Active Directory Authentication is blocked by default;  _Entitlements_, the new security system used by Elasticsearch, has a policy for the `x-pack-core` module that is too restrictive, and does not allow the LDAP library used for AD authentication to perform outbound network connections. \
The workaround is to patch the policy for `x-pack-core` to add a `outbound_network` entitlement to the LDAP library:

      unboundid.ldapsdk:
        - set_https_connection_properties
        - outbound_network
  This can be done by adding `-Des.entitlements.policy.x-pack-core=dmVyc2lvbnM6CiAgLSA4LjE4LjAKICAtIDkuMC4wCnBvbGljeToKICB1bmJvdW5kaWQubGRhcHNkazoKICAgIC0gc2V0X2h0dHBzX2Nvbm5lY3Rpb25fcHJvcGVydGllcwogICAgLSBvdXRib3VuZF9uZXR3b3Jr`to the JVM options for Elasticsearch. See the [JVM settings](https://www.elastic.co/docs/reference/elasticsearch/jvm-settings) docs.
