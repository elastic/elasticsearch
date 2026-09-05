/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.internal.info.BuildParameterService
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

class JmhPluginSpec extends Specification {

    Project consumer

    def setup() {
        // Sibling projects match JmhPlugin's findProject paths so the guarded wiring is exercised.
        def rootProject = ProjectBuilder.builder().withName("root").build()
        def benchmarks = ProjectBuilder.builder().withParent(rootProject).withName("benchmarks").build()
        ProjectBuilder.builder().withParent(benchmarks).withName("common").build()
        ProjectBuilder.builder().withParent(benchmarks).withName("processor").build()
        def testGroup = ProjectBuilder.builder().withParent(rootProject).withName("test").build()
        ProjectBuilder.builder().withParent(testGroup).withName("framework").build()

        consumer = ProjectBuilder.builder().withParent(rootProject).withName("consumer").build()

        // Empty stub for the buildParams shared service; the launcher chain is lazy, so an unset
        // extension is fine as long as we don't execute the task.
        consumer.gradle.sharedServices.registerIfAbsent("buildParams", BuildParameterService) { spec -> }

        consumer.pluginManager.apply(JmhPlugin)
    }

    def "applies the java plugin"() {
        expect:
        consumer.pluginManager.hasPlugin("java")
    }

    def "creates the benchmark and benchmarkTest source sets"() {
        when:
        def sourceSets = consumer.extensions.getByType(SourceSetContainer)

        then:
        sourceSets.findByName(JmhPlugin.BENCHMARK_SOURCE_SET) != null
        sourceSets.findByName(JmhPlugin.BENCHMARK_TEST_SOURCE_SET) != null
    }

    def "benchmark implementation extends main; benchmarkTest extends benchmark and test"() {
        // Structural check: iterating runtimeClasspath.files would force resolution of jmh-core.
        when:
        def conf = { String name -> consumer.configurations.getByName(name) }
        def benchmarkImpl = conf("benchmarkImplementation")
        def benchmarkTestImpl = conf("benchmarkTestImplementation")

        then:
        benchmarkImpl.extendsFrom.any { it.name == JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME }
        benchmarkTestImpl.extendsFrom.any { it.name == "benchmarkImplementation" }
        benchmarkTestImpl.extendsFrom.any { it.name == JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME }
    }

    def "registers a benchmark JavaExec task running the JMH main class"() {
        when:
        def task = consumer.tasks.getByName(JmhPlugin.BENCHMARK_TASK)

        then:
        task instanceof JavaExec
        ((JavaExec) task).mainClass.get() == "org.openjdk.jmh.Main"
        task.group == "benchmark"
    }

    def "registers a benchmarkTest Test task"() {
        when:
        def task = consumer.tasks.getByName(JmhPlugin.BENCHMARK_TEST_TASK)

        then:
        task instanceof Test
        task.group == "verification"
    }

    def "check depends on benchmarkTest and does not depend on benchmark"() {
        when:
        def check = consumer.tasks.getByName("check")
        def deps = check.taskDependencies.getDependencies(check).collect { it.name }

        then:
        JmhPlugin.BENCHMARK_TEST_TASK in deps
        // benchmark is developer-invoked; it must not be pulled in by check.
        (JmhPlugin.BENCHMARK_TASK in deps) == false
    }

    def "wires jmh-core on benchmarkImplementation and generator-annprocess on benchmarkAnnotationProcessor"() {
        when:
        def impl = consumer.configurations.getByName("benchmarkImplementation").dependencies
        def ap = consumer.configurations.getByName("benchmarkAnnotationProcessor").dependencies

        then:
        impl.any { it.group == "org.openjdk.jmh" && it.name == "jmh-core" }
        ap.any { it.group == "org.openjdk.jmh" && it.name == "jmh-generator-annprocess" }
    }

    def "redeclares jmh-core's stripped transitives on benchmarkRuntimeOnly"() {
        when:
        def rt = consumer.configurations.getByName("benchmarkRuntimeOnly").dependencies

        then:
        rt.any { it.group == "net.sf.jopt-simple" && it.name == "jopt-simple" }
        rt.any { it.group == "org.apache.commons" && it.name == "commons-math3" }
    }

    def "wires :benchmarks:common on benchmarkImplementation when present"() {
        when:
        def impl = consumer.configurations.getByName("benchmarkImplementation").dependencies

        then:
        impl.any { it instanceof ProjectDependency && ((ProjectDependency) it).path == ":benchmarks:common" }
    }

    def "wires :benchmarks:processor on benchmarkAnnotationProcessor when present"() {
        when:
        def ap = consumer.configurations.getByName("benchmarkAnnotationProcessor").dependencies

        then:
        ap.any { it instanceof ProjectDependency && ((ProjectDependency) it).path == ":benchmarks:processor" }
    }

    def "wires :test:framework on benchmarkTestImplementation when present"() {
        when:
        def impl = consumer.configurations.getByName("benchmarkTestImplementation").dependencies

        then:
        impl.any { it instanceof ProjectDependency && ((ProjectDependency) it).path == ":test:framework" }
    }
}
