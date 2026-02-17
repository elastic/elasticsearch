/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ElasticsearchTestBasePluginFuncTest extends AbstractGradleFuncTest {

    def "can disable assertions via cmdline param"() {
        given:
        file("src/test/java/acme/SomeTests.java").text = """
        public class SomeTests {
            @org.junit.Test
            public void testAsserts() {
                assert false;
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
        """

        when:
        def result = gradleRunner("test").buildAndFail()
        then:
        result.task(':test').outcome == TaskOutcome.FAILED

        when:
        result = gradleRunner("test", "-Dtests.asserts=false").build()
        then:
        result.task(':test').outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner("test", "-Dtests.jvm.argline=-da").build()
        then:
        result.task(':test').outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner("test", "-Dtests.jvm.argline=-disableassertions").build()
        then:
        result.task(':test').outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner("test", "-Dtests.asserts=false", "-Dtests.jvm.argline=-da").build()
        then:
        result.task(':test').outcome == TaskOutcome.SUCCESS
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

    def "uses new test seed for every invocation"() {
        given:
        file("src/test/java/acme/SomeTests.java").text = """

        public class SomeTests {
            @org.junit.Test
            public void printTestSeed() {
                System.out.println("TESTSEED=[" + System.getProperty("tests.seed") + "]");
            }
        }

        """
        buildFile.text = """
            plugins {
             id 'java'
             id 'elasticsearch.test-base'
            }

            tasks.named('test').configure {
                testLogging {
                    showStandardStreams = true
                }
            }

            tasks.register('test2', Test) {
                classpath = sourceSets.test.runtimeClasspath
                testClassesDirs = sourceSets.test.output.classesDirs
                testLogging {
                    showStandardStreams = true
                }
            }

            repositories {
                mavenCentral()
            }

            dependencies {
                testImplementation 'junit:junit:4.12'
            }

        """

        when:
        // Avoid invoking cleanTest* on Windows; it can flake due to file handle timing when deleting build/test-results/**/binary.
        // We still need two consecutive task executions to observe two different seeds while reusing the configuration cache.
        def result1 = gradleRunner("test", "test2", "--rerun-tasks").build()
        def result2 = gradleRunner("test", "test2", "--rerun-tasks").build()

        then:
        def seeds1 = result1.output.findAll(/(?m)TESTSEED=\[([^\]]+)\]/) { it[1] }
        def seeds2 = result2.output.findAll(/(?m)TESTSEED=\[([^\]]+)\]/) { it[1] }

        seeds1.unique().size() == 1
        seeds2.unique().size() == 1

        verifyAll {
            seeds1[0] != null
            seeds2[0] != null
            seeds1[0] != seeds2[0]
        }
        result2.output.contains("Configuration cache entry reused.")
    }
}
