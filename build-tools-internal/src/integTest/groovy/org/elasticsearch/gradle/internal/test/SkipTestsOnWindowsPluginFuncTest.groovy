/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class SkipTestsOnWindowsPluginFuncTest extends AbstractGradleFuncTest {
    def setup() {
        configurationCacheCompatible = false
        file("src/test/java/org/acme/Dummy.java") << """
            package org.acme;
            public class Dummy {}
        """
    }

    def "test task is skipped when simulating windows"() {
        given:
        buildFile.text = """
            plugins {
              id 'java'
              id 'elasticsearch.skip-tests-on-windows'
            }

            tasks.register("otherTest", Test) {
              testClassesDirs = tasks.named("test").get().testClassesDirs
              classpath = tasks.named("test").get().classpath
            }

            tasks.withType(Test).configureEach {
              failOnNoDiscoveredTests = false
            }
        """

        when:
        def result = gradleRunner(
            "test",
            "otherTest",
            "-Dos.name=Windows 11",
            "--rerun-tasks",
        ).build()

        then:
        result.task(":test").outcome == TaskOutcome.SKIPPED
        result.task(":otherTest").outcome == TaskOutcome.SKIPPED
    }

    def "test task runs on non-windows"() {
        given:
        buildFile.text = """
            plugins {
              id 'java'
              id 'elasticsearch.skip-tests-on-windows'
            }

            tasks.register("otherTest", Test) {
              testClassesDirs = tasks.named("test").get().testClassesDirs
              classpath = tasks.named("test").get().classpath
            }

            tasks.withType(Test).configureEach {
              failOnNoDiscoveredTests = false
            }
        """

        when:
        def result = gradleRunner("test", "otherTest", "--rerun-tasks").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.task(":otherTest").outcome == TaskOutcome.SUCCESS
    }
}

