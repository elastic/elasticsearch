/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

import groovy.json.JsonSlurper

/**
 * End-to-end functional test for the flakiness resolver's Gradle lifecycle. It proves the property the
 * prototype got wrong (JAVA_RESOLVER_NOTES.md P1a): the {@link FlakinessModelService} is populated from each
 * project's <em>own</em> configuration and read back by the {@code flakinessResolve} task at <em>execution</em>
 * time - so a real multi-project build resolves refs to correct, authoritative base targets rather than
 * silently resolving nothing.
 *
 * <p>The fixture is a two-project build:
 * <ul>
 *   <li>{@code :app} - a {@code test} source set with an abstract base and two concrete subclasses, to
 *       exercise ASM abstract-flattening on really-compiled bytecode;</li>
 *   <li>{@code :other} - a second project, to prove cross-project boundary resolution (longest projectDir
 *       prefix) over a service that holds more than one project.</li>
 * </ul>
 *
 * <p>Each subproject contributes its model via the exact registration snippet
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin} uses (registerIfAbsent +
 * {@link FlakinessProjectModel#contribute}, i.e. lazy configureEach/withPlugin, no afterEvaluate). Applying
 * the full {@code ElasticsearchTestBasePlugin} in the lightweight TestKit harness is impractical (it drags in
 * entitlements, test-rerun, etc.); the real plugin's apply path is instead verified by a full-build
 * {@code flakinessResolve} run (see JAVA_RESOLVER_NOTES.md Verification).
 *
 * <p>The three invocations mirror the three Gradle Buildkite steps: resolve, a plain compile of the emitted
 * task paths, then scan.
 */
class FlakinessResolvePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    @Override
    Class<? extends Plugin> getPluginClassUnderTest() {
        FlakinessResolvePlugin.class
    }

    def setup() {
        // The resolve step intrinsically requires the WHOLE build to be configured, because every test
        // project must contribute its own model to the shared service. Under the configuration cache Gradle
        // only configures the projects reachable from the requested root task, so the owning subprojects
        // would never configure and the service would be empty - which is exactly why CI runs
        // `flakinessResolve` with `--no-configuration-cache` (see JAVA_RESOLVER_NOTES.md). This func test
        // therefore disables the configuration cache to mirror how the step actually runs; it is listed in
        // IntegTestCoverageArchUnitSpec.KNOWN_CC_INCOMPATIBLE for the same reason.
        disableConfigurationCache("resolve requires whole-build configuration, which the configuration cache does not perform for a root task")

        // A subproject build script that applies the java plugin and contributes its own model to the
        // shared service exactly as ElasticsearchTestBasePlugin does under -Pflakiness.resolve: via the
        // lazy configureEach/withPlugin registration in FlakinessProjectModel.contribute - NO afterEvaluate.
        def contribute = """
            plugins { id 'java' }
            def flakinessModel = gradle.sharedServices.registerIfAbsent(
                org.elasticsearch.gradle.internal.flakiness.FlakinessModelService.NAME,
                org.elasticsearch.gradle.internal.flakiness.FlakinessModelService)
            org.elasticsearch.gradle.internal.flakiness.FlakinessProjectModel.contribute(project, flakinessModel)
        """

        subProject(":app") << contribute
        subProject(":other") << contribute

        // :app test hierarchy: AbstractFooTests (abstract) <- {BarTests, BazTests}, plus a standalone class.
        javaTestClass("app", "com/example/AbstractFooTests", "abstract class AbstractFooTests {}")
        javaTestClass("app", "com/example/BarTests", "class BarTests extends AbstractFooTests {}")
        javaTestClass("app", "com/example/BazTests", "class BazTests extends AbstractFooTests {}")

        // :other has an unrelated concrete test, referenced via a changed-file ref.
        javaTestClass("other", "com/other/OtherTests", "class OtherTests {}")
    }

    def "populates the model per-project and resolves, compiles and scans end-to-end"() {
        given: "a refs file with an abstract-base unmute (in :app) and a changed file (in :other)"
        file("flakiness-refs.json").text = """
            { "mergeBase": "test",
              "refs": [
                { "source": "unmute", "className": "com.example.AbstractFooTests" },
                { "source": "changed-file", "path": "other/src/test/java/com/other/OtherTests.java" } ] }
        """

        when: "resolve reads the service at execution time"
        def resolveResult = gradleRunner("-Pflakiness.resolve", "flakinessResolve").build()

        then: "it produced authoritative base targets across BOTH projects (not the silent-empty trap)"
        resolveResult.task(":flakinessResolve").outcome == TaskOutcome.SUCCESS
        def baseTargets = new JsonSlurper().parse(file("flakiness-base-targets.json"))
        baseTargets.targets.size() == 2
        baseTargets.unresolved.isEmpty()

        def appTarget = baseTargets.targets.find { it.gradleProject == ":app" }
        appTarget.fqcn == "com.example.AbstractFooTests"
        appTarget.sourceSet == "test"
        appTarget.kind == "test"
        appTarget.compileTaskPath == ":app:compileTestJava"
        appTarget.outputDir.replace('\\', '/').endsWith("app/build/classes/java/test")

        def otherTarget = baseTargets.targets.find { it.gradleProject == ":other" }
        otherTarget.fqcn == "com.other.OtherTests"
        otherTarget.compileTaskPath == ":other:compileTestJava"

        and: "the emitted compile task list covers both projects"
        def compileTasks = file("flakiness-compile-tasks.txt").text.readLines().findAll { it.trim() }
        compileTasks as Set == [":app:compileTestJava", ":other:compileTestJava"] as Set

        when: "the emitted compile tasks are run plainly, then scan enriches the compiled output"
        gradleRunner(compileTasks as String[]).build()
        def scanResult = gradleRunner("-Pflakiness.resolve", "flakinessScan").build()

        then: "the abstract base is flattened to its two concrete subclasses; the other test passes through"
        scanResult.task(":flakinessScan").outcome == TaskOutcome.SUCCESS
        def plan = new JsonSlurper().parse(file("flakiness-plan.json"))
        plan.buildFailed == false

        def expanded = plan.entries.findAll { it.expandedFrom == "com.example.AbstractFooTests" }
        expanded.collect { it.fqcn } as Set == ["com.example.BarTests", "com.example.BazTests"] as Set
        expanded.every { it.disposition == "run" }

        plan.expansions.size() == 1
        plan.expansions[0].abstractFqcn == "com.example.AbstractFooTests"
        plan.expansions[0].ran == 2

        def other = plan.entries.find { it.gradleProject == ":other" }
        other.fqcn == "com.other.OtherTests"
        other.disposition == "run"
        other.expandedFrom == null

        and: "the plan carries ready, target-neutral batch commands (Java owns batch-command generation)"
        plan.commands.size() >= 1
        plan.commands.every { it.command.contains("__GRADLE__") }
        // The three concrete unit tests (2 expanded + OtherTests) are all kind=test; capBatch=3 -> one batch.
        def unitCmd = plan.commands.find { it.key == "flakiness-detection:unit" }
        unitCmd != null
        unitCmd.command.startsWith("__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000!")
        unitCmd.command.contains("--tests com.example.BarTests")
        unitCmd.command.contains("--tests com.other.OtherTests")
    }

    private void javaTestClass(String project, String internalName, String body) {
        file("${project}/src/test/java/${internalName}.java").text = """
            package ${internalName.substring(0, internalName.lastIndexOf('/')).replace('/', '.')};
            ${body}
        """
    }
}
