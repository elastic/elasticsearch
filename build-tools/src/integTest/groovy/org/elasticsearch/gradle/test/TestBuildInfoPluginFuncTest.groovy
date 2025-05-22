package org.elasticsearch.gradle.test

import com.fasterxml.jackson.databind.ObjectMapper

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import java.nio.file.Path

class TestBuildInfoPluginFuncTest extends AbstractGradleFuncTest {
    def "basic functionality"() {
        given:
        file("src/main/java/com/example/Example.java") << """
            package com.example;

            public class Example {
            }
        """

        file("src/main/java/module-info.java") << """
            module com.example {
                exports com.example;
            }
        """

        buildFile << """
        import org.elasticsearch.gradle.plugin.GenerateTestBuildInfoTask;

        plugins {
            id 'java'
            id 'elasticsearch.test-build-info'
        }

        repositories {
            mavenCentral()
        }

        tasks.withType(GenerateTestBuildInfoTask.class) {
            componentName = 'example-component'
            outputFile = new File('build/generated-build-info/plugin-test-build-info.json')
        }
        """

        when:
        def result = gradleRunner('generateTestBuildInfo').build()
        def task = result.task(":generateTestBuildInfo")


        then:
        task.outcome == TaskOutcome.SUCCESS

        def output = file("build/generated-build-info/plugin-test-build-info.json")
        output.exists() == true

        def location = Map.of(
            "module", "com.example",
            "representative_class", "com/example/Example.class"
        )
        def expectedOutput = Map.of(
            "component", "example-component",
            "locations", List.of(location)
        )
        new ObjectMapper().readValue(output, Map.class) == expectedOutput
    }

    def "dependencies"() {
        buildFile << """
        import org.elasticsearch.gradle.plugin.GenerateTestBuildInfoTask;

        plugins {
            id 'java'
            id 'elasticsearch.test-build-info'
        }

        repositories {
            mavenCentral()
        }

        dependencies {
            // We pin to specific versions here because they are known to have the properties we want to test.
            // We're not actually running this code.
            implementation "org.ow2.asm:asm:9.7.1" // has module-info.class
            implementation "junit:junit:4.13" // has Automatic-Module-Name, and brings in hamcrest which does not
        }

        tasks.withType(GenerateTestBuildInfoTask.class) {
            componentName = 'example-component'
            outputFile = new File('build/generated-build-info/plugin-test-build-info.json')
        }
        """

        when:
        def result = gradleRunner('generateTestBuildInfo').build()
        def task = result.task(":generateTestBuildInfo")


        then:
        task.outcome == TaskOutcome.SUCCESS

        def output = file("build/generated-build-info/plugin-test-build-info.json")
        output.exists() == true

        def locationFromModuleInfo = Map.of(
            "module", "org.objectweb.asm",
            "representative_class", Path.of('org', 'objectweb', 'asm', 'AnnotationVisitor.class').toString()
        )
        def locationFromManifest = Map.of(
            "module", "junit",
            "representative_class", Path.of('junit', 'textui', 'TestRunner.class').toString()
        )
        def locationFromJarFileName = Map.of(
            "module", "hamcrest.core",
            "representative_class", Path.of('org', 'hamcrest', 'BaseDescription.class').toString()
        )
        def expectedOutput = Map.of(
            "component", "example-component",
            "locations", List.of(locationFromModuleInfo, locationFromManifest, locationFromJarFileName)
        )

        def value = new ObjectMapper().readValue(output, Map.class)
        expectedOutput.forEach((k,v) -> value.get(k) == v)
        value == expectedOutput
    }
}
