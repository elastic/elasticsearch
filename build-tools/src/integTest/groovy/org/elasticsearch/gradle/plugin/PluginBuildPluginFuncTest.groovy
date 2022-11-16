/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

class PluginBuildPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // underlaying TestClusterPlugin and StandaloneRestIntegTestTask are not cc compatible
        configurationCacheCompatible = false
    }

    def "can assemble plugin via #taskName"() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.esplugin'
            }

            esplugin {
                description = 'test plugin'
                classname = 'com.acme.plugin.TestPlugin'
            }

            // for testing purposes only
            configurations.compileOnly.dependencies.clear()
            """

        when:
        def result = gradleRunner(taskName).build()

        then:
        result.task(taskName).outcome == TaskOutcome.SUCCESS
        file(expectedOutputPath).exists()

        where:
        expectedOutputPath                    | taskName
        "build/distributions/hello-world.zip" | ":bundlePlugin"
        "build/explodedBundle/"               | ":explodedBundlePlugin"

    }

    def "can resolve plugin as directory without intermediate zipping "() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.esplugin'
            }

            esplugin {
                name = 'sample-plugin'
                description = 'test plugin'
                classname = 'com.acme.plugin.TestPlugin'
            }

            // for testing purposes only
            configurations.compileOnly.dependencies.clear()
            """

        file('settings.gradle') << "include 'module-consumer'"
        file('module-consumer/build.gradle') << """
            configurations {
                consume
            }

            dependencies {
                consume project(path:':', configuration:'${BasePluginBuildPlugin.EXPLODED_BUNDLE_CONFIG}')
            }

            tasks.register("resolveModule", Copy) {
                from configurations.consume
                into "build/resolved"
            }
        """
        when:
        def result = gradleRunner(":module-consumer:resolveModule").build()

        then:
        result.task(":module-consumer:resolveModule").outcome == TaskOutcome.SUCCESS
        result.task(":explodedBundlePlugin").outcome == TaskOutcome.SUCCESS
        file("module-consumer/build/resolved/sample-plugin.jar").exists()
        file("module-consumer/build/resolved/plugin-descriptor.properties").exists()
    }

    def "can build plugin properties"() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.esplugin'
            }

            version = '1.2.3'

            esplugin {
                name = 'myplugin'
                description = 'test plugin'
                classname = 'com.acme.plugin.TestPlugin'
            }
            """


        when:
        def result = gradleRunner(":pluginProperties").build()
        def props = getPluginProperties()

        then:
        result.task(":pluginProperties").outcome == TaskOutcome.SUCCESS
        props.get("name") == "myplugin"
        props.get("version") == "1.2.3"
        props.get("description") == "test plugin"
        props.get("classname") == "com.acme.plugin.TestPlugin"
        props.get("java.version") == Integer.toString(Runtime.version().feature())
        props.get("elasticsearch.version") == VersionProperties.elasticsearchVersion.toString()

        props.get("has.native.controller") == null
        props.get("extended.plugins") == null
        props.get("modulename") == null
        props.size() == 6
    }

    def "module name is inferred by plugin properties"() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.esplugin'
            }

            esplugin {
                name = 'myplugin'
                description = 'test plugin'
                classname = 'com.acme.plugin.TestPlugin'
            }

            // for testing purposes only
            configurations.compileOnly.dependencies.clear()
            """
        file('src/main/java/module-info.java') << """
            module org.test.plugin {
            }
        """

        when:
        def result = gradleRunner(":pluginProperties").build()
        def props = getPluginProperties()

        then:
        result.task(":pluginProperties").outcome == TaskOutcome.SUCCESS
        props.get("modulename") == "org.test.plugin"
    }

    Map<String, String> getPluginProperties() {
        Path propsFile = file("build/generated-descriptor/plugin-descriptor.properties").toPath();
        Properties rawProps = new Properties()
        try (var inputStream = Files.newInputStream(propsFile)) {
            rawProps.load(inputStream)
        }
        return rawProps.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()))
    }
}
