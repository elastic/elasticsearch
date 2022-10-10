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

class StableBuildPluginPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // underlaying TestClusterPlugin and StandaloneRestIntegTestTask are not cc compatible
        configurationCacheCompatible = false
    }

    def "can build stable plugin properties"() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.stable-esplugin'
            }

            version = '1.2.3'

            esplugin {
                name = 'myplugin'
                description = 'test plugin'
            }
            """

        when:
        def result = gradleRunner(":pluginProperties").build()
        def props = getPluginProperties()

        then:
        result.task(":pluginProperties").outcome == TaskOutcome.SUCCESS
        props.get("classname") == null

        props.get("name") == "myplugin"
        props.get("version") == "1.2.3"
        props.get("description") == "test plugin"
        props.get("modulename") == ""
        props.get("java.version") == Integer.toString(Runtime.version().feature())
        props.get("elasticsearch.version") == VersionProperties.elasticsearchVersion.toString()
        props.get("extended.plugins") == ""
        props.get("has.native.controller") == "false"
        props.size() == 8

    }

    def "can scan and create named components file"() {
        given:
        File jarFolder = new File(testProjectDir.root, "jars")
        jarFolder.mkdirs()

        buildFile << """plugins {
                id 'elasticsearch.stable-esplugin'
            }

            version = '1.2.3'

            esplugin {
                name = 'myplugin'
                description = 'test plugin'
            }

            dependencies {
                implementation files('${StableApiJarMocks.createPluginApiJar(jarFolder.toPath()).toAbsolutePath()}')
                implementation files('${StableApiJarMocks.createExtensibleApiJar(jarFolder.toPath()).toAbsolutePath()}')
            }

            """

        file("src/main/java/org/acme/A.java") << """
            package org.acme;

            import org.elasticsearch.plugin.api.NamedComponent;
            import org.elasticsearch.plugin.scanner.test_classes.ExtensibleClass;

            @NamedComponent(name = "componentA")
            public class A extends ExtensibleClass {
            }
        """


        when:
        def result = gradleRunner(":assemble").build()
        Path namedComponents = file("build/generated-named-components/named_components.json").toPath();
        def map = new JsonSlurper().parse(namedComponents.toFile())
        then:
        result.task(":assemble").outcome == TaskOutcome.SUCCESS

        map  == ["org.elasticsearch.plugin.scanner.test_classes.ExtensibleClass" : (["componentA" : "org.acme.A"]) ]
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
