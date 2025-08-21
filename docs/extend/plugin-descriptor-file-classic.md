---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-descriptor-file-classic.html
---

# The plugin descriptor file for classic plugins [plugin-descriptor-file-classic]

The classic plugin descriptor file is a Java properties file called `plugin-descriptor.properties` that describes the plugin. The file is automatically created if you are using {{es}}'s Gradle build system. If you’re not using the gradle plugin, you can create it manually using the following template.

```yaml
# Elasticsearch plugin descriptor file
# This file must exist as 'plugin-descriptor.properties' or 'stable-plugin-descriptor.properties inside a plugin.
#
## example plugin for "foo"
#
# foo.zip <-- zip file for the plugin, with this structure:
# |____   <arbitrary name1>.jar <-- classes, resources, dependencies
# |____   <arbitrary nameN>.jar <-- any number of jars
# |____   plugin-descriptor.properties <-- example contents below:
#
# classname=foo.bar.BazPlugin
# description=My cool plugin
# version=6.0
# elasticsearch.version=6.0
# java.version=1.8
#
## mandatory elements for all plugins:
#
# 'description': simple summary of the plugin
description=${description}
#
# 'version': plugin's version
version=${version}
#
# 'name': the plugin name
name=${name}
#
# 'java.version': version of java the code is built against
# use the system property java.specification.version
# version string must be a sequence of nonnegative decimal integers
# separated by "."'s and may have leading zeros
java.version=${javaVersion}
#
# 'elasticsearch.version': version of elasticsearch compiled against.
# Plugins implementing plugin-api.jar this version only has to match a major version of the ES server
# For all other plugins it has to be the same as ES server version
elasticsearch.version=${elasticsearchVersion}
## optional elements for plugins:
<% if (classname) { %>
#
# 'classname': the name of the class to load, fully-qualified. Only applies to
# "isolated" plugins
classname=${classname}
<% } %>
<% if (modulename) { %>
#
# 'modulename': the name of the module to load classname from. Only applies to
# "isolated" plugins. This is optional. Specifying it causes the plugin
# to be loaded as a module.
modulename=${modulename}
<% } %>
<% if (extendedPlugins) { %>
#
#  'extended.plugins': other plugins this plugin extends through SPI
extended.plugins=${extendedPlugins}
<% } %>
<% if (hasNativeController) { %>
#
# 'has.native.controller': whether or not the plugin has a native controller
has.native.controller=${hasNativeController}
<% } %>
<% if (licensed) { %>
# This plugin requires that a license agreement be accepted before installation
licensed=${licensed}
<% } %>
```


## Properties [_properties_2]

| Element | Type | Description |
| --- | --- | --- |
| `description` | String | simple summary of the plugin |
| `version` | String | plugin’s version |
| `name` | String | the plugin name |
| `classname` | String | the name of the class to load,fully-qualified. |
| `extended.plugins` | String | other plugins this plugin extends throughSPI. |
| `modulename` | String | the name of the module to load classnamefrom. Only applies to "isolated" plugins. This is optional. Specifying it causesthe plugin to be loaded as a module. |
| `java.version` | String | version of java the code is built against.Use the system property `java.specification.version`. Version string must be asequence of nonnegative decimal integers separated by "."'s and may have leadingzeros. |
| `elasticsearch.version` | String | version of {{es}} compiled against. |

