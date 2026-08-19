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
 * End-to-end functional test for the flakiness resolver's Gradle lifecycle, run with the <b>configuration
 * cache enabled</b> (the harness default - this test deliberately does not disable it).
 *
 * <p>It proves the two properties the design rests on:
 * <ol>
 *   <li><b>Self-selection.</b> {@code flakinessResolveProject} is registered in every project and invoked
 *       <em>unqualified</em>, so it runs everywhere; each project decides on its own whether a ref lands in
 *       one of its source sets. {@code :untouched} owns nothing and must write an empty result without
 *       realizing its {@code Test} tasks.</li>
 *   <li><b>Post-mutation correctness of the store-time capture.</b> {@code :bwcish} reproduces the shape of
 *       {@code elasticsearch.bwc-test} / {@code elasticsearch.distro-test}: the bare conventional task is
 *       disabled and differently named {@code Test} tasks are pointed at the SAME source-set output, both
 *       <em>after</em> the resolve task has been registered. An eager snapshot reads the pre-mutation values
 *       and wrongly emits the disabled bare task, so this fixture fails any capture that is not late.</li>
 * </ol>
 *
 * <p>The fixture is a four-project build:
 * <ul>
 *   <li>{@code :app} - a {@code test} source set with an abstract base and two concrete subclasses, to
 *       exercise ASM abstract-flattening on really-compiled bytecode;</li>
 *   <li>{@code :other} - a second project, to prove cross-project boundary resolution;</li>
 *   <li>{@code :bwcish} - the disabled-bare-task shape described above;</li>
 *   <li>{@code :untouched} - owns none of the refs.</li>
 * </ul>
 *
 * <p>Each subproject registers the task via the exact snippet
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin} uses
 * ({@link FlakinessProjectResolve#register}). Applying the full {@code ElasticsearchTestBasePlugin} in the
 * lightweight TestKit harness is impractical (it drags in entitlements, test-rerun, etc.); the real plugin's
 * apply path is instead verified by a full-build run (see JAVA_RESOLVER_NOTES.md Verification).
 *
 * <p>The three invocations mirror the three Gradle phases of the Buildkite orchestration step: the
 * unqualified resolve, a plain compile of the task paths the projects emitted, then scan.
 */
class FlakinessResolvePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    @Override
    Class<? extends Plugin> getPluginClassUnderTest() {
        FlakinessResolvePlugin.class
    }

    def setup() {
        // A subproject build script that applies the java plugin and registers its own resolve task exactly
        // as ElasticsearchTestBasePlugin does under -Pflakiness.resolve.
        def register = """
            plugins { id 'java' }
            org.elasticsearch.gradle.internal.flakiness.FlakinessProjectResolve.register(project, 'flakiness-refs.json', 2)
        """

        subProject(":app") << register
        subProject(":other") << register
        subProject(":untouched") << register

        // :bwcish flips the bare task off through `matching {}.configureEach {}` and registers the
        // alternatives AFTER the resolve task has been registered, exactly as bwc-test.gradle does.
        subProject(":bwcish") << register << """
            tasks.matching { it.name == 'test' }.configureEach { enabled = false }
            ['v9.6.0#altTest', 'v9.5.1#altTest', 'v9.4.0#altTest'].each { name ->
                tasks.register(name, Test) {
                    testClassesDirs = sourceSets.test.output.classesDirs
                    classpath = sourceSets.test.runtimeClasspath
                }
            }
        """

        // :app test hierarchy: AbstractFooTests (abstract) <- {BarTests, BazTests}, plus a standalone class.
        javaTestClass("app", "com/example/AbstractFooTests", "abstract class AbstractFooTests {}")
        javaTestClass("app", "com/example/BarTests", "class BarTests extends AbstractFooTests {}")
        javaTestClass("app", "com/example/BazTests", "class BazTests extends AbstractFooTests {}")

        // :other has an unrelated concrete test, referenced via a changed-file ref.
        javaTestClass("other", "com/other/OtherTests", "class OtherTests {}")

        javaTestClass("bwcish", "com/bwcish/BwcishTests", "class BwcishTests {}")

        // :untouched has tests, but no ref points at them - it must self-select OUT.
        javaTestClass("untouched", "com/untouched/UntouchedTests", "class UntouchedTests {}")
    }

    def "each project self-selects, resolves its own share, and the flow compiles and scans end-to-end"() {
        given: "a refs file with an abstract-base unmute (:app), a changed file (:other) and a disabled-bare-task project (:bwcish)"
        file("flakiness-refs.json").text = """
            { "mergeBase": "test",
              "refs": [
                { "source": "unmute", "className": "com.example.AbstractFooTests" },
                { "source": "changed-file", "path": "other/src/test/java/com/other/OtherTests.java" },
                { "source": "changed-file", "path": "bwcish/src/test/java/com/bwcish/BwcishTests.java" } ] }
        """

        when: "the resolve task is invoked UNQUALIFIED, so it runs in every project"
        def resolveResult = gradleRunner("-Pflakiness.resolve", "flakinessResolveProject").build()

        then: "every project ran it, including the one that owns nothing"
        [":app", ":other", ":bwcish", ":untouched"].every {
            resolveResult.task("${it}:flakinessResolveProject").outcome == TaskOutcome.SUCCESS
        }

        and: "the owning projects each wrote their own authoritative share"
        def app = projectTargets("app")
        app.resolved.size() == 1
        app.resolved[0].refIndex == 0
        def appTarget = app.resolved[0].target
        appTarget.gradleProject == ":app"
        appTarget.fqcn == "com.example.AbstractFooTests"
        appTarget.sourceSet == "test"
        appTarget.kind == "test"
        appTarget.compileTaskPath == ":app:compileTestJava"
        appTarget.outputDir.replace('\\', '/').endsWith("app/build/classes/java/test")
        // An ordinary project: the plain enabled bare task, derived from the model.
        appTarget.runnableTasks == [":app:test"]
        appTarget.skipReason == null

        def otherTarget = projectTargets("other").resolved[0].target
        otherTarget.gradleProject == ":other"
        otherTarget.fqcn == "com.other.OtherTests"
        otherTarget.compileTaskPath == ":other:compileTestJava"
        otherTarget.runnableTasks == [":other:test"]

        and: "the project that owns nothing wrote an EMPTY share and never realized its Test tasks"
        projectTargets("untouched").resolved.isEmpty()
        compileTasksOf("untouched").isEmpty()
        def untouchedModel = new JsonSlurper().parse(file("untouched/build/flakiness/project-model.json"))
        untouchedModel.ownsRefs == false
        // The cheap exit: no Test task was realized, so none is in the captured model.
        untouchedModel.testTasks.isEmpty()
        untouchedModel.sourceSets.isEmpty()

        and: "the disabled-bare-task project resolves to its real alternatives, newest-first and capped"
        def bwcishModel = new JsonSlurper().parse(file("bwcish/build/flakiness/project-model.json"))
        bwcishModel.ownsRefs == true
        // Proof the capture was late: the mutations applied after registration are visible.
        bwcishModel.testTasks.find { it.name == "test" }.enabled == false
        bwcishModel.testTasks.count { it.name.endsWith("#altTest") } == 3

        def bwcishTarget = projectTargets("bwcish").resolved[0].target
        bwcishTarget.runnableTasks == [":bwcish:v9.6.0#altTest", ":bwcish:v9.5.1#altTest"]
        bwcishTarget.runnableTasks.every { it != ":bwcish:test" }
        bwcishTarget.candidateTasks == 3
        bwcishTarget.skipReason == null

        and: "the per-project compile task lists, concatenated, cover all three owning projects"
        allCompileTasks() as Set == [":app:compileTestJava", ":other:compileTestJava", ":bwcish:compileTestJava"] as Set

        and: "the configuration cache entry was stored without problems"
        resolveResult.output.contains("Configuration cache entry stored")

        when: "the emitted compile tasks are run plainly, then scan enriches the compiled output"
        gradleRunner(allCompileTasks() as String[]).build()
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

        and: "the untouched project contributes nothing to the plan"
        plan.entries.every { it.gradleProject != ":untouched" }
        plan.unresolved.isEmpty()

        and: "the capped fan-out is reported"
        plan.taskSelections.size() == 1
        plan.taskSelections[0].gradleProject == ":bwcish"
        plan.taskSelections[0].total == 3
        plan.taskSelections[0].cap == 2
        plan.taskSelections[0].selected == [":bwcish:v9.6.0#altTest", ":bwcish:v9.5.1#altTest"]

        and: "the plan carries ready, target-neutral batch commands (Java owns batch-command generation)"
        plan.commands.size() >= 1
        plan.commands.every { it.command.contains("__GRADLE__") }
        // The three concrete unit tests (2 expanded + OtherTests) are all kind=test; capBatch=3 -> one batch.
        def unitCmd = plan.commands.find { it.key == "flakiness-detection:unit" }
        unitCmd != null
        unitCmd.command.startsWith("__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000!")
        unitCmd.command.contains("--tests com.example.BarTests")
        unitCmd.command.contains("--tests com.other.OtherTests")

        and: "the commands invoke the real alternative tasks, never the disabled bare one"
        def bwcishCmds = plan.commands.findAll { it.command.contains("com.bwcish.BwcishTests") }
        bwcishCmds.every { it.command.contains(":bwcish:test ") == false }
        bwcishCmds.collect { it.command }.join(" ").contains(":bwcish:v9.6.0#altTest --tests com.bwcish.BwcishTests")
        bwcishCmds.collect { it.command }.join(" ").contains(":bwcish:v9.5.1#altTest --tests com.bwcish.BwcishTests")
    }

    /**
     * A class ref no project owns must be reported as {@code unresolved} exactly once - the one verdict that
     * needs the global view, and the reason the per-project tasks throw their own away.
     */
    def "a class ref no project owns is reported unresolved by the scan step"() {
        given:
        file("flakiness-refs.json").text = """
            { "mergeBase": "test",
              "refs": [ { "source": "unmute", "className": "com.nowhere.GoneTests" } ] }
        """

        when:
        gradleRunner("-Pflakiness.resolve", "flakinessResolveProject").build()
        gradleRunner("-Pflakiness.resolve", "flakinessScan").build()

        then:
        def plan = new JsonSlurper().parse(file("flakiness-plan.json"))
        plan.entries.isEmpty()
        plan.unresolved.size() == 1
        plan.unresolved[0].ref.className == "com.nowhere.GoneTests"
        plan.unresolved[0].reason == "no-source-file"
    }

    private Object projectTargets(String project) {
        new JsonSlurper().parse(file("${FlakinessProjectResolve.TARGETS_DIR}/${project}.json"))
    }

    private List<String> compileTasksOf(String project) {
        file("${FlakinessProjectResolve.TARGETS_DIR}/${project}.compile-tasks.txt").text.readLines().findAll { it.trim() }
    }

    /** What the orchestration shell does: concatenate every project's compile task list. */
    private List<String> allCompileTasks() {
        file(FlakinessProjectResolve.TARGETS_DIR).listFiles()
            .findAll { it.name.endsWith(".compile-tasks.txt") }
            .collectMany { it.text.readLines() }
            .findAll { it.trim() }
            .unique()
            .sort()
    }

    private void javaTestClass(String project, String internalName, String body) {
        file("${project}/src/test/java/${internalName}.java").text = """
            package ${internalName.substring(0, internalName.lastIndexOf('/')).replace('/', '.')};
            ${body}
        """
    }
}
