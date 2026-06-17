/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.foreign

import org.elasticsearch.gradle.fixtures.AbstractJavaGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

/**
 * Exercises the plugin in a real Gradle invocation. ProjectBuilder cannot reliably observe the
 * interaction between this plugin's overrides and {@code ElasticsearchJavaBasePlugin}'s and
 * {@code ElasticsearchJavaModulePathPlugin}'s {@code configureEach} actions; those actions only
 * realize at task realization time inside a normal Gradle build, so the toolchain version and
 * module-path inference settings must be verified end-to-end here.
 */
class ForeignLibraryPluginFuncTest extends AbstractJavaGradleFuncTest {

    def setup() {
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.foreign-library'

            // The plugin guards its project deps with findProject(...) so this composite build,
            // which doesn't include the foreign-library projects, exercises the same code path as
            // the serverless composite. That's enough for the wiring checks below.

            tasks.register('inspectProcessorTask') {
              // Capture everything at configuration time as primitives — the configuration cache
              // forbids retaining Task references in execution-time closures.
              def proc = tasks.named('${ForeignLibraryPlugin.PROCESS_ANNOTATIONS_TASK_NAME}').get()
              def compile = tasks.named('compileJava').get()
              def procToolchain = proc.javaCompiler.get().metadata.languageVersion.asInt()
              def procSourceCompat = proc.sourceCompatibility
              def procTargetCompat = proc.targetCompatibility
              def procReleaseSet = proc.options.release.isPresent()
              def procInferModulePath = proc.modularity.inferModulePath.get()
              def procCompilerArgs = proc.options.compilerArgs.toList()
              def compileToolchain = compile.javaCompiler.get().metadata.languageVersion.asInt()
              def compileDependsOnProc = compile.taskDependencies.getDependencies(compile)
                  .collect { it.name }
                  .contains('${ForeignLibraryPlugin.PROCESS_ANNOTATIONS_TASK_NAME}')
              doLast {
                println "PROC_TOOLCHAIN=" + procToolchain
                println "PROC_SOURCE_COMPAT=" + procSourceCompat
                println "PROC_TARGET_COMPAT=" + procTargetCompat
                println "PROC_RELEASE_SET=" + procReleaseSet
                println "PROC_INFER_MODULE_PATH=" + procInferModulePath
                println "PROC_COMPILER_ARGS=" + procCompilerArgs
                println "COMPILE_TOOLCHAIN=" + compileToolchain
                println "COMPILE_DEPENDS_ON_PROC=" + compileDependsOnProc
              }
            }
        """
    }

    def "process task runs under JDK 25 toolchain while compileJava stays on the project's minimum"() {
        when:
        def result = gradleRunner('inspectProcessorTask', '-g', gradleUserHome).build()

        then:
        result.task(":inspectProcessorTask").outcome == TaskOutcome.SUCCESS
        result.output.contains("PROC_TOOLCHAIN=25")
        result.output.contains("PROC_SOURCE_COMPAT=25")
        result.output.contains("PROC_TARGET_COMPAT=25")
        // We intentionally leave --release unset on the processor task — pinning it to the
        // consumer's release would reject sources that reference now-finalized preview APIs.
        result.output.contains("PROC_RELEASE_SET=false")
        // ElasticsearchJavaModulePathPlugin disables inferModulePath globally; we re-enable it
        // for the processor task so it can resolve the consumer's `requires` clauses.
        result.output.contains("PROC_INFER_MODULE_PATH=true")
        result.output.contains("-proc:only")
        // compileJava continues to use the project's minimum runtime, not JDK 25.
        result.output.contains("COMPILE_TOOLCHAIN=21")
        // Hand-written sources never reference generated classes, so compileJava is independent.
        result.output.contains("COMPILE_DEPENDS_ON_PROC=false")
    }
}
