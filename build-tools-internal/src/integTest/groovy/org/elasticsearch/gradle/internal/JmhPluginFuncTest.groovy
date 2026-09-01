/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

class JmhPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = JmhPlugin

    def "creates benchmark and benchmarkTest source sets and their tasks"() {
        when:
        def result = gradleRunner('tasks', '--all').build()

        then:
        // compile<SourceSet>Java tasks are proxies for the source sets being wired
        result.output.contains('compileBenchmarkJava')
        result.output.contains('compileBenchmarkTestJava')
        result.output.contains('benchmark -')       // JavaExec runner task
        result.output.contains('benchmarkTest -')   // Test task
    }

    def "wires jmh-core, annotation processor, and stripped transitive deps"() {
        when:
        def impl = gradleRunner('dependencies', '--configuration', 'benchmarkImplementation').build().output
        def ap = gradleRunner('dependencies', '--configuration', 'benchmarkAnnotationProcessor').build().output
        def rt = gradleRunner('dependencies', '--configuration', 'benchmarkRuntimeOnly').build().output

        then:
        impl.contains('org.openjdk.jmh:jmh-core:')
        ap.contains('org.openjdk.jmh:jmh-generator-annprocess:')
        // jmh-core's transitive deps are stripped by ComponentMetadataRulesPlugin,
        // so the plugin must redeclare them explicitly.
        rt.contains('net.sf.jopt-simple:jopt-simple:')
        rt.contains('org.apache.commons:commons-math3:')
    }

    def "check runs benchmarkTest and does not run benchmark"() {
        given:
        // The plugin declares external JMH deps; the fixture project has no repositories
        // of its own, so we add mavenCentral here to let the annotation-processor and
        // runtime classpaths resolve when Gradle computes the task graph for `check`.
        buildFile << """
            repositories { mavenCentral() }
        """.stripIndent()

        when:
        def result = gradleRunner('check').build()

        then:
        // With no benchmark or test sources, the Test task should skip gracefully.
        def outcome = result.task(':benchmarkTest').outcome
        outcome == TaskOutcome.NO_SOURCE || outcome == TaskOutcome.SUCCESS
        // The benchmark runner is developer-invoked; check must not pull it in.
        result.task(':benchmark') == null
    }

}
