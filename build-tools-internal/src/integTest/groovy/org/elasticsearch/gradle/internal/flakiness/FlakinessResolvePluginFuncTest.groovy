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
 * <p>The fixture is a five-project build:
 * <ul>
 *   <li>{@code :app} - a {@code test} source set with an abstract base and two concrete subclasses, to
 *       exercise ASM abstract-flattening on really-compiled bytecode;</li>
 *   <li>{@code :downstream} - a THIRD concrete subclass of {@code :app}'s abstract base, in its own project.
 *       Only a repo-wide compile plus a repo-wide scan finds it, and it must then be reported rather than run
 *       under {@code :app}'s tasks (which do not contain it);</li>
 *   <li>{@code :other} - a second project, to prove cross-project boundary resolution;</li>
 *   <li>{@code :bwcish} - the disabled-bare-task shape described above;</li>
 *   <li>{@code :untouched} - owns none of the refs.</li>
 * </ul>
 *
 * <p>Each subproject applies {@link FlakinessProjectResolvePlugin} directly - the same plugin
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin} applies to every test project, so the
 * registration path under test is the real one. Applying the full {@code ElasticsearchTestBasePlugin} in the
 * lightweight TestKit harness is impractical (it drags in entitlements, test-rerun, etc.); that outer wiring is
 * instead verified by a full-build run (see JAVA_RESOLVER_NOTES.md Verification).
 *
 * <p>The three invocations mirror the three Gradle phases of the Buildkite orchestration step: the
 * unqualified resolve, an unqualified compile of every test source set, then scan.
 */
class FlakinessResolvePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    @Override
    Class<? extends Plugin> getPluginClassUnderTest() {
        FlakinessResolvePlugin.class
    }

    def setup() {
        // A subproject build script that applies the java plugin plus the per-project flakiness plugin, exactly
        // as ElasticsearchTestBasePlugin does. The plugin self-gates on -Pflakiness.resolve, so the invocations
        // below that omit the property (the plain compile) simply never see the task.
        def register = """
            plugins { id 'java' }
            pluginManager.apply(org.elasticsearch.gradle.internal.flakiness.FlakinessProjectResolvePlugin)
        """

        subProject(":app") << register
        subProject(":other") << register
        subProject(":untouched") << register

        // :downstream holds a concrete subclass of an abstract base that lives in ANOTHER project. This is the
        // case a subset compile cannot handle: unless :downstream's bytecode is also in the scan set, the base
        // looks like it has only the two subclasses in :app. It needs :app's test output on its compile
        // classpath, which in this lightweight fixture is the direct source-set-output dependency.
        subProject(":downstream") << register << """
            evaluationDependsOn(':app')
            // Hoisted out of `dependencies {}` on purpose: in there, `project(':app')` is the dependency
            // factory method and returns a ProjectDependency, which has no `sourceSets`.
            def appTestOutput = project(':app').sourceSets.test.output
            dependencies { testImplementation appTestOutput }
        """

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
        // public, so the cross-project subclass in :downstream can extend it from another package.
        javaTestClass("app", "com/example/AbstractFooTests", "public abstract class AbstractFooTests {}")
        javaTestClass("app", "com/example/BarTests", "class BarTests extends AbstractFooTests {}")
        javaTestClass("app", "com/example/BazTests", "class BazTests extends AbstractFooTests {}")

        // ...and a THIRD subclass, in a different project. Only a repo-wide compile + repo-wide scan finds it.
        javaTestClass("downstream", "com/downstream/DownstreamTests", """
            import com.example.AbstractFooTests;
            class DownstreamTests extends AbstractFooTests {}
        """)

        // A concrete subclass that is NOT a test: a helper sharing the source set, plus an anonymous subclass
        // inside it. Both are concrete in bytecode, so expansion finds them, but `--tests <helper>` and
        // `--tests Foo$1` match nothing. They must be reported, not run.
        javaTestClass("downstream", "com/downstream/DownstreamHelper", """
            import com.example.AbstractFooTests;
            class DownstreamHelper extends AbstractFooTests {
                static final AbstractFooTests ANON = new AbstractFooTests() {};
            }
        """)

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
        // An ordinary project: the plain enabled bare task, derived from the model.
        appTarget.runnableTasks == [":app:test"]
        appTarget.skipReason == null

        def otherTarget = projectTargets("other").resolved[0].target
        otherTarget.gradleProject == ":other"
        otherTarget.fqcn == "com.other.OtherTests"
        otherTarget.runnableTasks == [":other:test"]

        and: "the projects that own nothing resolved nothing, but still reported a full model"
        projectTargets("untouched").resolved.isEmpty()
        projectTargets("downstream").resolved.isEmpty()
        def untouchedModel = new JsonSlurper().parse(file("untouched/build/flakiness/project-model.json"))
        // No cheap exit any more: owning no ref does not make a project irrelevant, because the scan may need
        // to run a subclass compiled here. So its source sets AND its Test tasks are captured.
        !untouchedModel.sourceSets.isEmpty()
        untouchedModel.testTasks.find { it.name == "test" } != null

        and: "every project reports how each of its test source sets can be re-run"
        def downstreamDisp = projectTargets("downstream").dispositions
        downstreamDisp.size() == 1
        downstreamDisp[0].sourceSet == "test"
        downstreamDisp[0].kind == "test"
        downstreamDisp[0].runnableTasks == [":downstream:test"]
        downstreamDisp[0].skipReason == null

        and: "...but they DID report their class dirs, which is what makes the scan repo-wide"
        classDirsOf("untouched").any { it.endsWith("untouched/build/classes/java/test") }
        classDirsOf("downstream").any { it.endsWith("downstream/build/classes/java/test") }
        // main is in the set too: abstract bases live in main source sets in the real repo, and the scan can
        // only call a class abstract if it visited that class's own .class file.
        classDirsOf("app").any { it.endsWith("app/build/classes/java/main") }

        and: "the disabled-bare-task project resolves to its real alternatives, newest-first and capped"
        def bwcishModel = new JsonSlurper().parse(file("bwcish/build/flakiness/project-model.json"))
        // Proof the capture was late: the mutations applied after registration are visible.
        bwcishModel.testTasks.find { it.name == "test" }.enabled == false
        bwcishModel.testTasks.count { it.name.endsWith("#altTest") } == 3

        def bwcishTarget = projectTargets("bwcish").resolved[0].target
        bwcishTarget.runnableTasks == [":bwcish:v9.6.0#altTest", ":bwcish:v9.5.1#altTest"]
        bwcishTarget.runnableTasks.every { it != ":bwcish:test" }
        bwcishTarget.candidateTasks == 3
        bwcishTarget.skipReason == null

        and: "the class-dir union spans EVERY project, not just the three that own a ref"
        [":app", ":other", ":bwcish", ":untouched", ":downstream"].every { p ->
            allClassDirs().any { it.endsWith("${p.substring(1)}/build/classes/java/test") }
        }

        and: "the configuration cache entry was stored without problems"
        resolveResult.output.contains("Configuration cache entry stored")

        when: "every test source set is compiled UNQUALIFIED (nothing read back from resolve), then scan runs"
        gradleRunner("compileTestJava").build()
        def scanResult = gradleRunner("-Pflakiness.resolve", "flakinessScan").build()

        then: "the repo-wide scan FINDS all three concrete subclasses, including the one in another project"
        scanResult.task(":flakinessScan").outcome == TaskOutcome.SUCCESS
        def plan = new JsonSlurper().parse(file("flakiness-plan.json"))
        plan.buildFailed == false

        plan.expansions.size() == 1
        plan.expansions[0].abstractFqcn == "com.example.AbstractFooTests"
        // 3, not 2: com.downstream.DownstreamTests lives in :downstream and was invisible to a subset scan.
        // This count is the regression test for the repo-wide compile + scan.
        // 5 concrete descendants exist in bytecode (2 in :app, DownstreamTests, DownstreamHelper and the
        // anonymous DownstreamHelper$1); the cap is 5, so all are expanded and then classified below.
        plan.expansions[0].total == 5

        and: "the two subclasses in the base's own output run under the base target's real tasks"
        def sameProject = plan.entries.findAll { it.expandedFrom == "com.example.AbstractFooTests" && it.gradleProject == ":app" }
        sameProject.collect { it.fqcn } as Set == ["com.example.BarTests", "com.example.BazTests"] as Set
        sameProject.every { it.disposition == "run" && it.runnableTasks == [":app:test"] }

        and: "the non-test subclasses are reported, never run"
        // A helper and an anonymous class are concrete in bytecode but nothing a Test task can address.
        def notTests = plan.entries.findAll { it.reason == "not-a-test-class" }
        notTests.collect { it.fqcn } as Set == ["com.downstream.DownstreamHelper", "com.downstream.DownstreamHelper\$1"] as Set
        notTests.every { it.disposition == "skip" }
        plan.commands.every { !it.command.contains("DownstreamHelper") }

        and: "the cross-project subclass is RE-HOMED onto its own project's task, not the base's"
        // :app:test does not contain com.downstream.DownstreamTests, so inheriting the base's tasks would emit
        // `:app:test --tests com.downstream.DownstreamTests` - zero tests run, looks like a hang. It must be
        // attributed to :downstream, whose own Test task really executes it. This is only possible because
        // :downstream reported its source-set disposition despite owning no ref.
        def crossProject = plan.entries.findAll { it.fqcn == "com.downstream.DownstreamTests" }
        crossProject.size() == 1
        crossProject[0].disposition == "run"
        crossProject[0].gradleProject == ":downstream"
        crossProject[0].sourceSet == "test"
        crossProject[0].runnableTasks == [":downstream:test"]
        crossProject[0].expandedFrom == "com.example.AbstractFooTests"

        and: "and it is invoked under :downstream:test in the emitted commands, never under :app:test"
        def downstreamCmds = plan.commands.findAll { it.command.contains("com.downstream.DownstreamTests") }
        downstreamCmds.size() >= 1
        downstreamCmds.collect { it.command }.join(" ").contains(":downstream:test --tests com.downstream.DownstreamTests")
        !plan.commands.any { it.command =~ /:app:test[^:]*--tests com\.downstream/ }

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
        // Four concrete unit tests are kind=test now (BarTests, BazTests, the re-homed DownstreamTests and
        // OtherTests), and capBatch=3, so they slice into two batches rather than one.
        def unitCmds = plan.commands.findAll { it.key == "flakiness-detection:unit" }
        unitCmds.size() == 2
        unitCmds.every { it.command.startsWith("__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000!") }
        def allUnit = unitCmds.collect { it.command }.join(" ")
        allUnit.contains("--tests com.example.BarTests")
        allUnit.contains("--tests com.other.OtherTests")

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
        new JsonSlurper().parse(file("${FlakinessProjectResolvePlugin.TARGETS_DIR}/${project}.json"))
    }

    private List<String> classDirsOf(String project) {
        projectTargets(project).classDirs.collect { (it as String).replace('\\', '/') }
    }

    /** What the scan step does: union every project's class dirs, owners and non-owners alike. */
    private List<String> allClassDirs() {
        file(FlakinessProjectResolvePlugin.TARGETS_DIR).listFiles()
            .findAll { it.name.endsWith(".json") }
            .collectMany { new JsonSlurper().parse(it).classDirs.collect { d -> (d as String).replace('\\', '/') } }
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
