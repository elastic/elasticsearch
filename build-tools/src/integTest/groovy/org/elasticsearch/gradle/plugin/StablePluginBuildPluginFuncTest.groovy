/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin

import groovy.json.JsonSlurper

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.internal.test.StableApiJarMocks
import org.gradle.testkit.runner.TaskOutcome

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

import static org.elasticsearch.gradle.fixtures.TestClasspathUtils.setupNamedComponentScanner

class StablePluginBuildPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile << """plugins {
                id 'elasticsearch.stable-esplugin'
            }

            version = '1.2.3'

            esplugin {
                name = 'myplugin'
                description = 'test plugin'
            }
            repositories {
              maven {
                name = "local-test"
                url = file("local-repo")
                metadataSources {
                  artifact()
                }
              }
            }
            """

        // underlaying TestClusterPlugin and StandaloneRestIntegTestTask are not cc compatible
        configurationCacheCompatible = false

        def version = VersionProperties.elasticsearch
        setupNamedComponentScanner(dir("local-repo/org/elasticsearch/elasticsearch-plugin-scanner/${version}/"), version)

    }

    def "can build stable plugin properties"() {
        given:
        when:
        def result = gradleRunner(":pluginProperties").build()
        def props = getPluginProperties()

        then:
        result.task(":pluginProperties").outcome == TaskOutcome.SUCCESS

        props.get("name") == "myplugin"
        props.get("version") == "1.2.3"
        props.get("description") == "test plugin"
        props.get("java.version") == Integer.toString(Runtime.version().feature())
        props.get("elasticsearch.version") == VersionProperties.elasticsearchVersion.toString()

        props.get("classname") == null
        props.get("modulename") == null
        props.get("extended.plugins") == null
        props.get("has.native.controller") == null
        props.size() == 5

    }

    def "can scan and create named components file"() {
        //THIS IS RUNNING A MOCK CONFIGURED IN setup()
        given:
        File jarFolder = new File(testProjectDir.root, "jars")
        jarFolder.mkdirs()

        buildFile << """
            dependencies {
                implementation files('${normalized(StableApiJarMocks.createPluginApiJar(jarFolder.toPath()).toAbsolutePath().toString())}')
                implementation files('${normalized(StableApiJarMocks.createExtensibleApiJar(jarFolder.toPath()).toAbsolutePath().toString())}')
            }
            """

        file("src/main/java/org/acme/A.java") << """
            package org.acme;

            import org.elasticsearch.plugin.NamedComponent;
            import org.elasticsearch.plugin.scanner.test_classes.ExtensibleClass;

            @NamedComponent( "componentA")
            public class A extends ExtensibleClass {
            }
        """

        when:
        def result = gradleRunner(":assemble", "-i").build()

        then:
        result.task(":assemble").outcome == TaskOutcome.SUCCESS
        //we expect that a Fake namedcomponent scanner used in this test will be passed a filename to be created
        File namedComponents = file("build/generated-named-components/named_components.json")
        namedComponents.exists() == true
    }

    Map<String, String> getPluginProperties() {
        Path propsFile = file("build/generated-descriptor/stable-plugin-descriptor.properties").toPath();
        Properties rawProps = new Properties()
        try (var inputStream = Files.newInputStream(propsFile)) {
            rawProps.load(inputStream)
        }
        return rawProps.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()))
    }
}
