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

/**
 * The {@code heapPressureTest} task is deliberately not part of {@code check}, so regular CI never runs it. This test
 * guards its wiring at configuration time, without starting a cluster.
 */
class InternalHeapPressureTestPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = InternalHeapPressureTestPlugin

    def setup() {
        // RestTestBasePlugin resolves the BWC versions when applied, so provide a minimal, internally consistent graph
        // that matches the version.properties of the test fixture.
        file('server/src/main/java/org/elasticsearch/Version.java').text = """
            package org.elasticsearch;
            public class Version {
                public static final Version V_8_18_0 = new Version(8_18_00_99);
                public static final Version V_9_0_9 = new Version(9_00_09_99);
                public static final Version V_9_1_0 = new Version(9_01_00_99);
                public static final Version CURRENT = V_9_1_0;
                private Version(int id) {}
            }
        """.stripIndent()
        def branches = file('synthetic-branches.json')
        branches.text = """
            {
              "branches": [
                { "branch": "main", "version": "9.1.0" },
                { "branch": "9.0", "version": "9.0.9" }
              ]
            }
        """.stripIndent()
        propertiesFile << """
            org.elasticsearch.build.branches-file-location=${branches.absolutePath}
        """
    }

    def "registers heapPressureTest as an isolated REST integ test outside check"() {
        given:
        buildFile << """
            def heapPressureTest = tasks.named('heapPressureTest', Test).get()

            assert restTests.tasks*.name.contains('heapPressureTest')
            assert heapPressureTest.class.name.contains('StandaloneRestIntegTestTask') == false
            assert heapPressureTest.testClassesDirs.files == sourceSets.heapPressureTest.output.classesDirs.files
            assert heapPressureTest.classpath.files == sourceSets.heapPressureTest.runtimeClasspath.files
            // each class runs in its own JVM so heap-constrained clusters are fully isolated
            assert heapPressureTest.forkEvery == 1L

            def check = tasks.named('check').get()
            assert check.taskDependencies.getDependencies(check).contains(heapPressureTest) == false

            // Replaces the --debug-server-jvm option of the legacy task type: tests.* system properties
            // are passed on to the test JVM, where the JUnit rule based cluster enables debugging.
            assert heapPressureTest.systemProperties['tests.cluster.debug.enabled'] == 'true'
        """

        when:
        def result = gradleRunner('help', '-Dtests.cluster.debug.enabled=true').build()

        then:
        result.task(':help').outcome == TaskOutcome.SUCCESS
    }
}
