/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Unroll

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ElasticsearchTestBasePluginFuncTest extends AbstractGradleFuncTest {

    private void setupAssertionTest() {
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
                testImplementation 'junit:junit:4.13.2'
                testImplementation 'org.junit.jupiter:junit-jupiter:6.1.3'
                testRuntimeOnly 'org.junit.vintage:junit-vintage-engine'
                testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
            }
        """
    }

    def "assertions are enabled by default"() {
        given:
        setupAssertionTest()

        when:
        def result = gradleRunner("test").buildAndFail()
        then:
        result.task(':test').outcome == TaskOutcome.FAILED
    }

    @Unroll
    def "can disable assertions via #description"() {
        given:
        setupAssertionTest()

        when:
        def result = gradleRunner(*gradleArgs).build()
        then:
        result.task(':test').outcome == TaskOutcome.SUCCESS

        where:
        description                            | gradleArgs
        "tests.asserts=false"                  | ["test", "-Dtests.asserts=false"]
        "tests.jvm.argline=-da"                | ["test", "-Dtests.jvm.argline=-da"]
        "tests.jvm.argline=-disableassertions" | ["test", "-Dtests.jvm.argline=-disableassertions"]
        "tests.asserts=false and -da combined" | ["test", "-Dtests.asserts=false", "-Dtests.jvm.argline=-da"]
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
                testImplementation 'junit:junit:4.13.2'
                testImplementation 'org.junit.jupiter:junit-jupiter:6.1.3'
                testRuntimeOnly 'org.junit.vintage:junit-vintage-engine'
                testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
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
                testImplementation 'junit:junit:4.13.2'
                testImplementation 'org.junit.jupiter:junit-jupiter:6.1.3'
                testRuntimeOnly 'org.junit.vintage:junit-vintage-engine'
                testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
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

    def "JUnit Vintage warning is not printed"() {
        given:
        setupAssertionTest()

        when:
        def result = gradleRunner("test").buildAndFail()

        then:
        result.task(':test').outcome == TaskOutcome.FAILED

        !result.output.contains(
            "The JUnit Vintage engine is deprecated and should only be used temporarily while migrating tests to JUnit Jupiter"
        )
    }

    @Unroll
    def "--tests '#filter' selects #selected of a parameterized randomized runner test"() {
        given:
        // The fixture compiles a copy of the runner of the Elasticsearch tests, as it cannot be a dependency of this build, see the
        // integTest task definition in build.gradle
        file("src/test/java/org/elasticsearch/test/junit/runner/ESRandomizedRunner.java").text =
            new File(System.getProperty("es.randomized.runner.source")).text
        file("src/test/java/org/acme/ParameterizedTests.java").text = """
            package org.acme;

            import com.carrotsearch.randomizedtesting.annotations.Name;
            import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
            import java.util.List;
            import org.elasticsearch.test.junit.runner.ESRandomizedRunner;
            import org.junit.Test;
            import org.junit.runner.RunWith;

            @RunWith(ESRandomizedRunner.class)
            public class ParameterizedTests {

                public ParameterizedTests(@Name("phase") int phase) {
                }

                @ParametersFactory(shuffle = false)
                public static Iterable<Object[]> parameters() {
                    return List.of(new Object[] { 0 }, new Object[] { 1 }, new Object[] { 2 });
                }

                @Test
                public void someTest() {
                }

                @Test
                public void otherTest() {
                }
            }
        """.stripIndent()
        buildFile.text = """
            plugins {
             id 'java'
             id 'elasticsearch.test-base'
            }

            repositories {
                mavenCentral()
            }

            dependencies {
                testImplementation 'junit:junit:4.13.2'
                testImplementation 'com.carrotsearch.randomizedtesting:randomizedtesting-runner:2.8.2'
                testImplementation 'org.junit.jupiter:junit-jupiter:6.1.3'
                testRuntimeOnly 'org.junit.vintage:junit-vintage-engine'
                testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
            }

            tasks.named('test').configure {
                testLogging { events "started" }
            }
        """

        when:
        def result = gradleRunner("test", "--tests", filter).build()

        then:
        result.task(':test').outcome == TaskOutcome.SUCCESS
        ["someTest {phase=0}", "someTest {phase=1}", "someTest {phase=2}", "otherTest {phase=0}", "otherTest {phase=1}", "otherTest {phase=2}"]
            .findAll { result.output.contains(it + " STARTED") } == expected

        where:
        filter                                            | selected                     | expected
        "org.acme.ParameterizedTests.someTest {phase=1}"  | "only that variant"          | ["someTest {phase=1}"]
        "org.acme.ParameterizedTests.otherTest {phase=2}" | "only that variant"          | ["otherTest {phase=2}"]
        "org.acme.ParameterizedTests.someTest*"           | "all variants of the method" |
            ["someTest {phase=0}", "someTest {phase=1}", "someTest {phase=2}"]
    }
}
