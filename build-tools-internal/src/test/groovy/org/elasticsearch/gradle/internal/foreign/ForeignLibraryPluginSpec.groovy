/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.foreign

import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.plugins.JavaLibraryPlugin
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

class ForeignLibraryPluginSpec extends Specification {

    Project consumer
    Project foreignLibraryProject
    Project processorProject

    def setup() {
        def rootProject = ProjectBuilder.builder().withName("root").build()
        def libsProject = ProjectBuilder.builder().withParent(rootProject).withName("libs").build()
        foreignLibraryProject = ProjectBuilder.builder().withParent(libsProject).withName("foreign-library").build()
        processorProject = ProjectBuilder.builder().withParent(foreignLibraryProject).withName("processor").build()
        consumer = ProjectBuilder.builder().withParent(rootProject).withName("consumer").build()

        // Apply java-library to the stub libs so they expose the api/runtime configurations
        // that the consumer project resolves through.
        foreignLibraryProject.pluginManager.apply(JavaLibraryPlugin)
        processorProject.pluginManager.apply(JavaLibraryPlugin)

        consumer.pluginManager.apply(ForeignLibraryPlugin)
    }

    def "applies java-library plugin"() {
        expect:
        consumer.pluginManager.hasPlugin("java-library")
    }

    def "declares foreign-library as an API dependency"() {
        when:
        def api = consumer.configurations.getByName(JavaPlugin.API_CONFIGURATION_NAME)
        def dep = api.dependencies.iterator().next()

        then:
        dep instanceof ProjectDependency
        ((ProjectDependency) dep).path == ":libs:foreign-library"
    }

    def "creates processor configuration with the processor project as its dependency"() {
        when:
        def proc = consumer.configurations.getByName(ForeignLibraryPlugin.PROCESSOR_CONFIGURATION_NAME)
        def dep = proc.dependencies.iterator().next()

        then:
        dep instanceof ProjectDependency
        ((ProjectDependency) dep).path == ":libs:foreign-library:processor"
    }

    def "registers processForeignAnnotations as a JavaCompile with the expected wiring"() {
        when:
        def task = (JavaCompile) consumer.tasks.getByName(ForeignLibraryPlugin.PROCESS_ANNOTATIONS_TASK_NAME)

        then:
        task.destinationDirectory.get().asFile == new File(consumer.layout.buildDirectory.get().asFile, "generated-foreign-library-classes")
        "-proc:only" in task.options.compilerArgs
        task.options.annotationProcessorPath == consumer.configurations.getByName(ForeignLibraryPlugin.PROCESSOR_CONFIGURATION_NAME)
        // The processor library uses java.lang.classfile (JDK 24+) and may reference now-finalized
        // preview APIs, so the plugin pins the toolchain to JDK 25 and leaves --release unset.
        task.sourceCompatibility == "25"
        task.targetCompatibility == "25"
        task.options.release.isPresent() == false
        // The plugin re-enables inferModulePath, which ElasticsearchJavaModulePathPlugin disables.
        task.modularity.inferModulePath.get() == true
    }

    def "registers augmentForeignModuleInfo with the expected wiring"() {
        when:
        def task = (AugmentForeignModuleInfoTask) consumer.tasks.getByName(ForeignLibraryPlugin.AUGMENT_MODULE_INFO_TASK_NAME)
        def buildDir = consumer.layout.buildDirectory.get().asFile

        then:
        task.outputModuleInfo.get().asFile == new File(buildDir, "augmented-foreign-module-info/module-info.class")
        task.servicesFile.get().asFile == new File(buildDir, "generated-foreign-library-classes/META-INF/services/org.elasticsearch.foreign.LibraryProvider")
        // input module-info points at compileJava's output
        def compileJava = (JavaCompile) consumer.tasks.getByName(JavaPlugin.COMPILE_JAVA_TASK_NAME)
        task.inputModuleInfo.get().asFile == new File(compileJava.destinationDirectory.get().asFile, "module-info.class")
    }

    def "compileJava is independent of processForeignAnnotations"() {
        when:
        def compileJava = (JavaCompile) consumer.tasks.getByName(JavaPlugin.COMPILE_JAVA_TASK_NAME)
        def deps = compileJava.taskDependencies.getDependencies(compileJava).collect { it.name }

        then:
        // Generated $Provider/$Impl classes are loaded reflectively via ServiceLoader; hand-written
        // sources never reference them, so the compilations are independent.
        ForeignLibraryPlugin.PROCESS_ANNOTATIONS_TASK_NAME !in deps
    }

    def "main source set output includes the generated classes dir"() {
        when:
        def sourceSets = consumer.extensions.getByType(SourceSetContainer)
        def main = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME)
        def expected = new File(consumer.layout.buildDirectory.get().asFile, "generated-foreign-library-classes")

        then:
        expected in main.output.dirs.files
    }

    def "jar task depends on augmentForeignModuleInfo"() {
        when:
        def jar = (Jar) consumer.tasks.getByName(JavaPlugin.JAR_TASK_NAME)
        def deps = jar.taskDependencies.getDependencies(jar).collect { it.name }

        then:
        ForeignLibraryPlugin.AUGMENT_MODULE_INFO_TASK_NAME in deps
    }
}
