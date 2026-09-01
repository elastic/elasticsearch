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

    def "creates jmh and jmhTest source sets and their tasks"() {
        when:
        def result = gradleRunner('tasks', '--all').build()

        then:
        // compile<SourceSet>Java tasks are proxies for the source sets being wired
        result.output.contains('compileJmhJava')
        result.output.contains('compileJmhTestJava')
        result.output.contains('jmh -')       // JavaExec runner task
        result.output.contains('jmhTest -')   // Test task
    }

    def "wires jmh-core, annotation processor, and stripped transitive deps"() {
        given:
        buildFile << """
            tasks.register('printJmhDeps') {
                def impl = configurations.jmhImplementation.dependencies.collect {
                    "\${it.group}:\${it.name}:\${it.version}"
                }.sort()
                def ap = configurations.jmhAnnotationProcessor.dependencies.collect {
                    "\${it.group}:\${it.name}:\${it.version}"
                }.sort()
                def rt = configurations.jmhRuntimeOnly.dependencies.collect {
                    "\${it.group}:\${it.name}:\${it.version}"
                }.sort()
                doLast {
                    println "JMH_IMPL=" + impl.join(';')
                    println "JMH_AP=" + ap.join(';')
                    println "JMH_RT=" + rt.join(';')
                }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('printJmhDeps').build()

        then:
        def impl = extract(result.output, 'JMH_IMPL=')
        def ap = extract(result.output, 'JMH_AP=')
        def rt = extract(result.output, 'JMH_RT=')

        impl.any { it.startsWith('org.openjdk.jmh:jmh-core:') }
        ap.any { it.startsWith('org.openjdk.jmh:jmh-generator-annprocess:') }
        // jmh-core's transitive deps are stripped by ComponentMetadataRulesPlugin,
        // so the plugin must redeclare them explicitly.
        rt.any { it.startsWith('net.sf.jopt-simple:jopt-simple:') }
        rt.any { it.startsWith('org.apache.commons:commons-math3:') }
    }

    def "check runs jmhTest and does not run jmh"() {
        when:
        def result = gradleRunner('check').build()

        then:
        // With no benchmark or test sources, the Test task should skip gracefully.
        def outcome = result.task(':jmhTest').outcome
        outcome == TaskOutcome.NO_SOURCE || outcome == TaskOutcome.SUCCESS
        // The benchmark runner is developer-invoked; check must not pull it in.
        result.task(':jmh') == null
    }

    private static List<String> extract(String output, String prefix) {
        def line = output.readLines().find { it.startsWith(prefix) }
        assert line != null : "missing line starting with '$prefix' in:\n$output"
        def payload = line.substring(prefix.length())
        payload.isEmpty() ? [] : payload.split(';') as List
    }
}
