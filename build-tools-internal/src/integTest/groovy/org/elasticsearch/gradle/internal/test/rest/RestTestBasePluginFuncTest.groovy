/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest

class RestTestBasePluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        disableConfigurationCache("esplugin plugin is not configuration cache compatible yet")

        settingsFile.text = """
        plugins {
            id 'elasticsearch.java-toolchain'
        }

        toolchainManagement {
          jvm {
            javaRepositories {
              repository('bundledOracleOpendJdk') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.OracleOpenJdkToolchainResolver
              }
              repository('adoptiumJdks') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.AdoptiumJdkToolchainResolver
              }
              repository('archivedOracleJdks') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.ArchivedOracleJdkToolchainResolver
              }
            }
          }
        }
        """ + settingsFile.text

        internalBuild()
        subProject(":server") << """
            plugins {
                id 'java'
            }
        """
        subProject(":test:framework") << """
            plugins {
                id 'java'
            }
        """
    }

    def "resolves module and plugin dependencies from the registry"() {
        given:
        pluginProject(":modules:a", "renamed-a", "org.example.modules.a.RenamedAPlugin")
        pluginProject(":modules:b", "renamed-b", "org.example.modules.b.RenamedBPlugin", ["renamed-a"])
        pluginProject(":plugins:c", "renamed-c", "org.example.plugins.c.RenamedCPlugin")
        restConsumerProject([":modules:b"], [":plugins:c"])

        when:
        def result = gradleRunner(":consumer:resolveClusterArtifacts").build()

        then:
        result.output.contains("RESOLVED_MODULES=renamed-a,renamed-b")
        result.output.contains("RESOLVED_PLUGINS=renamed-c-")
        result.output.contains(".zip")
    }

    def "resolves extended modules declared by non-module plugins"() {
        given:
        pluginProject(":modules:a", "renamed-a", "org.example.modules.a.RenamedAPlugin")
        pluginProject(":plugins:d", "renamed-d", "org.example.plugins.d.RenamedDPlugin", ["renamed-a"])
        restConsumerProject([], [":plugins:d"])

        when:
        def result = gradleRunner(":consumer:resolveClusterArtifacts").build()

        then:
        result.output.contains("RESOLVED_MODULES=")
        result.output.contains("RESOLVED_PLUGINS=renamed-a-")
        result.output.contains("renamed-d-")
        result.output.contains(".zip")
    }

    def "fails when extended module lookup is ambiguous"() {
        given:
        pluginProject(":modules:a", "duplicate-name", "org.example.modules.a.DuplicateAPlugin")
        pluginProject(":modules:b", "duplicate-name", "org.example.modules.b.DuplicateBPlugin", ["duplicate-name"])
        restConsumerProject([":modules:b"], [])

        when:
        def result = gradleRunner(":consumer:resolveClusterArtifacts").buildAndFail()

        then:
        result.output.contains("Ambiguous module plugin name [duplicate-name]")
        result.output.contains(":modules:a")
        result.output.contains(":modules:b")
    }

    private void pluginProject(String projectPath, String pluginName, String classname, List<String> extendedPlugins = []) {
        String extendedPluginsAssignment = extendedPlugins.isEmpty() ? "" : "extendedPlugins = ${asGroovyStringList(extendedPlugins)}"
        subProject(projectPath) << """
            plugins {
                id 'elasticsearch.base-internal-es-plugin'
            }

            esplugin {
                name = '${pluginName}'
                description = '${pluginName} description'
                classname = '${classname}'
                ${extendedPluginsAssignment}
            }
        """
    }

    private void restConsumerProject(List<String> moduleDependencies, List<String> pluginDependencies) {
        String moduleDependencyLines = moduleDependencies.collect { "clusterModules project('${it}')" }.join("\n        ")
        String pluginDependencyLines = pluginDependencies.collect { "clusterPlugins project('${it}')" }.join("\n        ")

        subProject(":consumer") << """
            plugins {
                id 'elasticsearch.internal-java-rest-test'
            }

            dependencies {
                ${moduleDependencyLines}
                ${pluginDependencyLines}
            }

            def clusterModulesConfiguration = configurations.clusterModules
            def clusterPluginsConfiguration = configurations.clusterPlugins

            tasks.register('resolveClusterArtifacts') {
                inputs.files(clusterModulesConfiguration, clusterPluginsConfiguration)
                doLast {
                    def modules = clusterModulesConfiguration.files.collect { it.name }.sort().join(',')
                    def plugins = clusterPluginsConfiguration.files.collect { it.name }.sort().join(',')
                    println "RESOLVED_MODULES=\${modules}"
                    println "RESOLVED_PLUGINS=\${plugins}"
                }
            }
        """
    }

    private static String asGroovyStringList(List<String> values) {
        return "[" + values.collect { "'${it}'" }.join(", ") + "]"
    }
}
