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

class StableBuildPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // underlaying TestClusterPlugin and StandaloneRestIntegTestTask are not cc compatible
        configurationCacheCompatible = false
    }

    def "can build stable plugin properties"() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.stable_esplugin'
            }

            version = '1.2.3'

            esplugin {
                name = 'myplugin'
                description = 'test plugin'
            }
            """

        when:
        def result = gradleRunner(":pluginProperties").build()
        def props = getPluginProperties("build/generated-descriptor/stable-plugin-descriptor.properties")

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


    Map<String, String> getPluginProperties(String fileName) {
        Path propsFile = file(fileName).toPath();
        Properties rawProps = new Properties()
        try (var inputStream = Files.newInputStream(propsFile)) {
            rawProps.load(inputStream)
        }
        return rawProps.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()))
    }
}
