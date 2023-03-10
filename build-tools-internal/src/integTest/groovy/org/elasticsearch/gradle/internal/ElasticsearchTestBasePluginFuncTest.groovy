/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ElasticsearchTestBasePluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // see https://github.com/gradle/gradle/issues/24172
        configurationCacheCompatible = false
    }

    def "can configure nonInputProperties for test tasks"() {
        given:
        file("src/test/java/acme/SomeTests.java").text = """

        public class SomeTests {
            @org.junit.Test
            public void testSysInput() {
                org.junit.Assert.assertEquals("bar", System.getProperty("foo"));
            }
        }

        """
        buildFile.text = """
            plugins {
             id 'java'
             id 'elasticsearch.test-base'
            }

            repositories {
                mavenCentral()
            }

            dependencies {
                testImplementation 'junit:junit:4.12'
            }

            tasks.named('test').configure {
                nonInputProperties.systemProperty("foo", project.getProperty('foo'))
            }
        """

        when:
        def result = gradleRunner("test", '-Dtests.seed=default', '-Pfoo=bar').build()

        then:
        result.task(':test').outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner("test", '-i', '-Dtests.seed=default', '-Pfoo=baz').build()

        then:
        result.task(':test').outcome == TaskOutcome.UP_TO_DATE
    }
}
