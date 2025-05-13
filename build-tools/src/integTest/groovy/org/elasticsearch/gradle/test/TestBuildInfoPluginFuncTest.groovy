package org.elasticsearch.gradle.test

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class TestBuildInfoPluginFuncTest extends AbstractGradleFuncTest{
    def "works"() {
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
        plugins {
            id 'elasticsearch.test-build-info'
            id 'java'
        }

        repositories {
            mavenCentral()
        }

        dependencies {
        }
        """

        when:
        def result = gradleRunner('generateTestBuildInfo').build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
    }
}
