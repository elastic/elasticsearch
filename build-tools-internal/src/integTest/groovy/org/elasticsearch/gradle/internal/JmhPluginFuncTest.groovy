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

/**
 * Verifies the wiring performed by {@link JmhPlugin}: the two source sets exist, the two
 * tasks are registered with the expected shape, and {@code jmhTest} participates in
 * {@code check}.
 */
class JmhPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = JmhPlugin

    def "creates jmh and jmhTest source sets"() {
        given:
        buildFile << """
            tasks.register('printSourceSets') {
                def ssNames = sourceSets*.name.sort()
                doLast { println "SOURCESETS=" + ssNames.join(",") }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('printSourceSets').build()

        then:
        def line = result.output.readLines().find { it.startsWith("SOURCESETS=") }
        line != null
        line.contains("jmh")
        line.contains("jmhTest")
    }

    def "compileJmhJava and compileJmhTestJava tasks exist"() {
        // The Java plugin auto-creates a compile<SourceSet>Java task per source set,
        // so their presence is a proxy for the source sets being wired correctly.
        when:
        def result = gradleRunner('tasks', '--all').build()

        then:
        result.output.contains("compileJmhJava")
        result.output.contains("compileJmhTestJava")
    }

    def "jmh runner task is a JavaExec with the JMH main class"() {
        given:
        buildFile << """
            tasks.register('printJmhTask') {
                def t = tasks.named('jmh').get()
                def isJavaExec = t instanceof JavaExec
                def mc = t.mainClass.get()
                doLast { println "JMH_TASK: isJavaExec=" + isJavaExec + " mainClass=" + mc }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('printJmhTask').build()

        then:
        result.output.contains("JMH_TASK: isJavaExec=true mainClass=org.openjdk.jmh.Main")
    }

    def "jmhTest task is a Test task and check depends on it, but jmh is not"() {
        given:
        buildFile << """
            tasks.register('printJmhTestWiring') {
                def jmhTestTask = tasks.named('jmhTest').get()
                def isTest = jmhTestTask instanceof Test
                def checkDeps = tasks.named('check').get().taskDependencies.getDependencies(null)*.name.sort()
                doLast {
                    println "JMH_TEST_WIRING: isTest=" + isTest + " checkDependsOn=" + checkDeps.join(",")
                }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('printJmhTestWiring').build()

        then:
        def line = result.output.readLines().find { it.startsWith("JMH_TEST_WIRING:") }
        line != null
        line.contains("isTest=true")
        // The list of `check` dependencies, extracted from `checkDependsOn=<csv>`.
        def deps = line.replaceFirst(".*checkDependsOn=", "").split(",") as List
        "jmhTest" in deps
        !("jmh" in deps)  // benchmarks are developer-invoked, not part of `check`
    }
}
