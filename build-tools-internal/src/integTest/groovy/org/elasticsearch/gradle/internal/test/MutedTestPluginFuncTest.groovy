/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

class MutedTestPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = MutedTestPlugin

    def setup() {
        buildFile << """
            apply plugin: 'java'
            repositories { mavenCentral() }
            dependencies {
                testImplementation 'junit:junit:4.13.2'
                testImplementation 'com.carrotsearch.randomizedtesting:randomizedtesting-runner:2.8.2'
                testImplementation 'org.junit.jupiter:junit-jupiter:6.1.3'
                testRuntimeOnly 'org.junit.vintage:junit-vintage-engine'
                testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
            }
            tasks.withType(Test).configureEach {
                useJUnitPlatform()
                testLogging { events "started" }
            }
        """
    }

    def "muted tests are excluded by default"() {
        given:
        muteTest("org.acme.SomeTest", "someMutedTest")
        randomizedClass("src/test/java", "org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("someMutedTest STARTED") == false
        result.output.contains("someUnmutedTest STARTED")
    }

    def "tests.mutes.enabled=true applies mutes explicitly"() {
        given:
        muteTest("org.acme.SomeTest", "someMutedTest")
        randomizedClass("src/test/java", "org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test", "-Dtests.mutes.enabled=true").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("someMutedTest STARTED") == false
        result.output.contains("someUnmutedTest STARTED")
    }

    def "tests.mutes.enabled=false runs all tests including muted ones"() {
        given:
        muteTest("org.acme.SomeTest", "someMutedTest")
        randomizedClass("src/test/java", "org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test", "-Dtests.mutes.enabled=false").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("someMutedTest STARTED")
        result.output.contains("someUnmutedTest STARTED")
    }

    def "task scoped mutes only apply to listed tasks"() {
        given:
        addOtherTestSourceSet()
        muteTest("org.acme.ScopedTest", "someMutedTest", [":otherTest"])
        randomizedClass("src/test/java", "org.acme.ScopedTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            """
        }
        randomizedClass("src/otherTest/java", "org.acme.ScopedTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def mainTaskResult = gradleRunner("test").build()
        def scopedTaskResult = gradleRunner("otherTest").build()

        then:
        mainTaskResult.task(":test").outcome == TaskOutcome.SUCCESS
        mainTaskResult.output.contains("someMutedTest STARTED")
        scopedTaskResult.task(":otherTest").outcome == TaskOutcome.SUCCESS
        scopedTaskResult.output.contains("someMutedTest STARTED") == false
        scopedTaskResult.output.contains("someUnmutedTest STARTED")
    }

    def "adding a scoped mute only reexecutes the affected test task"() {
        given:
        addOtherTestSourceSet()
        randomizedClass("src/test/java", "org.acme.UnaffectedTest") {
            """
            @org.junit.Test public void unaffectedTest() {}
            """
        }
        randomizedClass("src/otherTest/java", "org.acme.ScopedTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def initialResult = gradleRunner("test", "otherTest").build()
        muteTest("org.acme.ScopedTest", "someMutedTest", [":otherTest"])
        def secondResult = gradleRunner("test", "otherTest").build()

        then:
        initialResult.task(":test").outcome == TaskOutcome.SUCCESS
        initialResult.task(":otherTest").outcome == TaskOutcome.SUCCESS

        secondResult.task(":test").outcome == TaskOutcome.UP_TO_DATE
        secondResult.task(":otherTest").outcome == TaskOutcome.SUCCESS
        secondResult.output.contains("someMutedTest STARTED") == false
        secondResult.output.contains("someUnmutedTest STARTED")
        secondResult.output.contains("unaffectedTest STARTED") == false
    }

    def "muting a single parameter of a parameterized test leaves its siblings running"() {
        given:
        muteTest("org.acme.ParameterizedTest", "someTest {phase=1}")
        file("src/test/java/org/elasticsearch/test/junit/runner/ESRandomizedRunner.java").text = esRandomizedRunnerSource()
        file("src/test/java/org/acme/ParameterizedTest.java").text = """
            package org.acme;

            import com.carrotsearch.randomizedtesting.annotations.Name;
            import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
            import java.util.List;
            import org.elasticsearch.test.junit.runner.ESRandomizedRunner;
            import org.junit.Test;
            import org.junit.runner.RunWith;

            @RunWith(ESRandomizedRunner.class)
            public class ParameterizedTest {

                public ParameterizedTest(@Name("phase") int phase) {
                }

                @ParametersFactory(shuffle = false)
                public static Iterable<Object[]> parameters() {
                    return List.of(new Object[] { 0 }, new Object[] { 1 }, new Object[] { 2 });
                }

                @Test
                public void someTest() {
                }
            }
        """.stripIndent()

        when:
        def result = gradleRunner("test").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("someTest {phase=0} STARTED")
        result.output.contains("someTest {phase=1} STARTED") == false
        result.output.contains("someTest {phase=2} STARTED")
    }

    def "invalid task scoped mute schema fails the build"() {
        given:
        file("muted-tests.yml").text = """
            tests:
            - class: org.acme.SomeTest
              method: someMutedTest
              tasks: []
              issue: https://github.com/elastic/elasticsearch/issues/1
        """.stripIndent()
        randomizedClass("src/test/java", "org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test").buildAndFail()

        then:
        result.output.contains("muted test tasks must not be empty")
    }

    /**
     * Writes a test class run by the randomized runner of the Elasticsearch tests, like the Elasticsearch tests are. The fixture project
     * compiles a copy of that runner, because it cannot be a dependency of this build, see the integTest task definition in build.gradle.
     * The body is the content of the class, whose methods must use the fully qualified name of the JUnit annotations.
     */
    private File randomizedClass(String sourceDir, String className, Closure<String> body) {
        file("${sourceDir}/org/elasticsearch/test/junit/runner/ESRandomizedRunner.java").text = esRandomizedRunnerSource()

        int lastDot = className.lastIndexOf('.')
        File sourceFile = file("${sourceDir}/${className.replace('.', '/')}.java")
        sourceFile.text = """
            package ${className.substring(0, lastDot)};

            import org.elasticsearch.test.junit.runner.ESRandomizedRunner;
            import org.junit.runner.RunWith;

            @RunWith(ESRandomizedRunner.class)
            public class ${className.substring(lastDot + 1)} {
            ${body.call()}
            }
        """.stripIndent()
        sourceFile
    }

    /** The source of the runner of the Elasticsearch tests, handed over by the integTest task. */
    private static String esRandomizedRunnerSource() {
        new File(System.getProperty("es.randomized.runner.source")).text
    }

    private void addOtherTestSourceSet() {
        buildFile << """
            sourceSets {
                otherTest {
                    java.srcDir 'src/otherTest/java'
                    compileClasspath += sourceSets.main.output + configurations.testRuntimeClasspath
                    runtimeClasspath += output + compileClasspath
                }
            }
            configurations {
                otherTestImplementation.extendsFrom testImplementation
                otherTestRuntimeOnly.extendsFrom testRuntimeOnly
            }
            tasks.register('otherTest', Test) {
                testClassesDirs = sourceSets.otherTest.output.classesDirs
                classpath = sourceSets.otherTest.runtimeClasspath
            }
        """
    }

    private void muteTest(String className, String method, List<String> tasks = null) {
        String tasksBlock = tasks == null ? "" : "\n  tasks:\n" + tasks.collect { "  - ${it}" }.join("\n")
        file("muted-tests.yml").text = """tests:
- class: ${className}
  method: ${method}
  issue: https://github.com/elastic/elasticsearch/issues/1${tasksBlock}
"""
    }
}
