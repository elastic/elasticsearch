/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.PlanEntry;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link CommandBuilder} - the Java port of the old TypeScript {@code commands.ts} batching +
 * per-kind command generation. Assert the target-neutral {@code __GRADLE__} marker, per-kind command shapes,
 * cap-batching, the dedupe/collapse/dedup-runners staging, and the {@code FLAKINESS_ITERS} override.
 */
public class CommandBuilderTests {

    @Test
    public void testUnitTestsBatchByCapWithItersAndTimeout() {
        List<PlanEntry> run = List.of(unit(":server", "org.A"), unit(":server", "org.B"), unit(":server", "org.C"), unit(":server", "org.D"));

        List<PlanCommand> cmds = CommandBuilder.build(run, CommandBuilder.Config.defaults());

        // cap for `test` is 3 -> two batches.
        assertThat(cmds, hasSize(2));
        PlanCommand first = cmds.get(0);
        assertThat(first.kind(), equalTo("test"));
        assertThat(first.label(), equalTo("unit tests"));
        assertThat(first.key(), equalTo("flakiness-detection:unit"));
        assertThat(
            first.command(),
            equalTo("__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.A --tests org.B --tests org.C")
        );
        assertThat(cmds.get(1).command(), equalTo("__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.D"));
        // The task paths travel as a field so the batch runner can scope its skipped-task check to exactly
        // the tasks this command invokes, without parsing them back out of the command string.
        assertThat(first.taskPaths(), contains(":server:test"));
    }

    /**
     * A batch spanning several tasks must carry every one of them: the runner only treats a zero-test result
     * as {@code not_applicable} when ALL the tasks it asked for came back SKIPPED, so a missing path would
     * make that check silently unsatisfiable.
     */
    @Test
    public void testTaskPathsCoverEveryTaskInTheBatchWithoutDuplicates() {
        List<PlanEntry> run = List.of(unit(":a", "org.A"), unit(":b", "org.B"), unit(":a", "org.C"));

        List<PlanCommand> cmds = CommandBuilder.build(run, CommandBuilder.Config.defaults());

        assertThat(cmds, hasSize(1));
        assertThat(cmds.get(0).taskPaths(), contains(":a:test", ":b:test"));
        for (String taskPath : cmds.get(0).taskPaths()) {
            assertThat(cmds.get(0).command(), containsString(taskPath));
        }
    }

    @Test
    public void testIterOverrideAppliesUniformly() {
        List<PlanEntry> run = List.of(unit(":server", "org.A"), integ(":server", "org.BIT"), javaRest(":server", "org.CIT"));

        CommandBuilder.Config cfg = CommandBuilder.Config.defaults().withIterOverride(7);
        List<PlanCommand> cmds = CommandBuilder.build(run, cfg);

        assertThat(byKey(cmds, "flakiness-detection:unit").command(), containsString("-Dtests.iters=7 "));
        assertThat(byKey(cmds, "flakiness-detection:integ").command(), containsString("-Dtests.iters=7 "));
        // rest kinds use the repeat-rest wrapper with the override as the loop count.
        assertThat(
            byKey(cmds, "flakiness-detection:java-rest").command(),
            containsString(".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 7 __GRADLE__ ")
        );
    }

    @Test
    public void testNonPositiveOrNullIterOverrideKeepsDefaults() {
        assertThat(CommandBuilder.Config.defaults().withIterOverride(null).testIters(), is(100));
        assertThat(CommandBuilder.Config.defaults().withIterOverride(0).testIters(), is(100));
        assertThat(CommandBuilder.Config.defaults().withIterOverride(-3).restIters(), is(10));
    }

    @Test
    public void testJavaRestUsesRepeatWrapperAndRerun() {
        List<PlanCommand> cmds = CommandBuilder.build(List.of(javaRest(":x", "org.FooIT")), CommandBuilder.Config.defaults());
        assertThat(cmds, hasSize(1));
        assertThat(
            cmds.get(0).command(),
            equalTo(".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 __GRADLE__ :x:javaRestTest --tests org.FooIT --rerun")
        );
    }

    @Test
    public void testYamlRunnersDedupedPerProject() {
        List<PlanCommand> cmds = CommandBuilder.build(
            List.of(yamlRunner(":x"), yamlRunner(":x"), yamlRunner(":y")),
            CommandBuilder.Config.defaults()
        );
        // Two projects -> two runner commands (the duplicate for :x is dropped), cap 1 each.
        assertThat(cmds, hasSize(2));
        assertThat(cmds.get(0).command(), containsString(":x:yamlRestTest --rerun"));
        assertThat(cmds.get(1).command(), containsString(":y:yamlRestTest --rerun"));
    }

    @Test
    public void testYamlSuitesInSameDirCollapseToDirectory() {
        List<PlanCommand> cmds = CommandBuilder.build(
            List.of(yamlSuite(":x", "esql/10_a"), yamlSuite(":x", "esql/20_b")),
            CommandBuilder.Config.defaults()
        );
        assertThat(cmds, hasSize(1));
        // Two files under esql/ collapse to the directory-level suite.
        assertThat(cmds.get(0).command(), containsString("-Dtests.rest.suite.:x:yamlRestTest=esql"));
        assertThat(cmds.get(0).command(), not(containsString("10_a")));
    }

    @Test
    public void testYamlCaseUsesQuotedFqcnDotYamlTest() {
        List<PlanCommand> cmds = CommandBuilder.build(
            List.of(yamlCase(":x", "org.EsqlIT", "test {yaml=esql/10_a/Case}")),
            CommandBuilder.Config.defaults()
        );
        assertThat(cmds, hasSize(1));
        assertThat(cmds.get(0).command(), containsString("--tests \"org.EsqlIT.test {yaml=esql/10_a/Case}\""));
        assertThat(cmds.get(0).command(), containsString("--rerun"));
    }

    @Test
    public void testDedupeCollapsesIdenticalTargets() {
        List<PlanCommand> cmds = CommandBuilder.build(List.of(unit(":s", "org.A"), unit(":s", "org.A")), CommandBuilder.Config.defaults());
        assertThat(cmds, hasSize(1));
        // Only one --tests org.A after dedupe.
        assertThat(cmds.get(0).command(), equalTo("__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :s:test --tests org.A"));
    }

    /**
     * The bwc shape: the bare {@code javaRestTest} task is disabled, so the entry carries the capped
     * {@code v<version>#bwcTest} tasks. Each becomes its own batch command (javaRestTest cap is 1), which is
     * what keeps per-task attribution clean for the analyzer - one bwc version per Buildkite job.
     */
    @Test
    public void testMultiTaskEntryEmitsOneCommandPerTask() {
        PlanEntry bwc = runOn(
            ":qa:rolling",
            "javaRestTest",
            Kinds.JAVA_REST_TEST,
            "org.FooIT",
            null,
            null,
            List.of(":qa:rolling:v9.6.0#bwcTest", ":qa:rolling:v9.5.1#bwcTest")
        );

        List<PlanCommand> cmds = CommandBuilder.build(List.of(bwc), CommandBuilder.Config.defaults());

        assertThat(cmds, hasSize(2));
        assertThat(cmds.get(0).command(), containsString(":qa:rolling:v9.6.0#bwcTest --tests org.FooIT --rerun"));
        assertThat(cmds.get(1).command(), containsString(":qa:rolling:v9.5.1#bwcTest --tests org.FooIT --rerun"));
        // Never the disabled bare task.
        assertThat(cmds.get(0).command(), not(containsString(":qa:rolling:javaRestTest")));
    }

    /** A multi-task unit-test entry shares one invocation when the per-kind batch cap allows it. */
    @Test
    public void testMultiTaskUnitEntryBatchesTasksIntoOneInvocation() {
        PlanEntry twoTasks = runOn(":qa:p", "test", Kinds.TEST, "org.ATests", null, null, List.of(":qa:p:altTestA", ":qa:p:altTestB"));

        List<PlanCommand> cmds = CommandBuilder.build(List.of(twoTasks), CommandBuilder.Config.defaults());

        assertThat(cmds, hasSize(1));
        assertThat(
            cmds.get(0).command(),
            equalTo(
                "__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! "
                    + ":qa:p:altTestA --tests org.ATests :qa:p:altTestB --tests org.ATests"
            )
        );
    }

    @Test
    public void testKindOrderingAcrossKinds() {
        List<PlanCommand> cmds = CommandBuilder.build(
            List.of(yamlCase(":x", "org.Y", "test {yaml=a/b}"), unit(":x", "org.U"), integ(":x", "org.I")),
            CommandBuilder.Config.defaults()
        );
        // Emitted in KIND_ORDER: test, internalClusterTest, ..., yamlRestTestCase.
        assertThat(
            cmds.stream().map(PlanCommand::key).toList(),
            contains("flakiness-detection:unit", "flakiness-detection:integ", "flakiness-detection:yaml-case")
        );
    }

    // ---- fixtures ----

    /**
     * A conventional entry: its single runnable task is the bare {@code :project:<sourceSet>} task, which is
     * what the resolver derives for a project that does not disable it.
     */
    private static PlanEntry run(String project, String sourceSet, String kind, String fqcn, String suitePath, String yamlTest) {
        return runOn(project, sourceSet, kind, fqcn, suitePath, yamlTest, List.of(project + ":" + sourceSet));
    }

    private static PlanEntry runOn(
        String project,
        String sourceSet,
        String kind,
        String fqcn,
        String suitePath,
        String yamlTest,
        List<String> runnableTasks
    ) {
        return new PlanEntry(project, sourceSet, kind, fqcn, suitePath, yamlTest, Kinds.DISPOSITION_RUN, null, null, runnableTasks);
    }

    private static PlanEntry unit(String project, String fqcn) {
        return run(project, "test", Kinds.TEST, fqcn, null, null);
    }

    private static PlanEntry integ(String project, String fqcn) {
        return run(project, "internalClusterTest", Kinds.INTERNAL_CLUSTER_TEST, fqcn, null, null);
    }

    private static PlanEntry javaRest(String project, String fqcn) {
        return run(project, "javaRestTest", Kinds.JAVA_REST_TEST, fqcn, null, null);
    }

    private static PlanEntry yamlRunner(String project) {
        return run(project, "yamlRestTest", Kinds.YAML_REST_TEST_RUNNER, null, null, null);
    }

    private static PlanEntry yamlSuite(String project, String suitePath) {
        return run(project, "yamlRestTest", Kinds.YAML_REST_TEST_SUITE, null, suitePath, null);
    }

    private static PlanEntry yamlCase(String project, String fqcn, String yamlTest) {
        return run(project, "yamlRestTest", Kinds.YAML_REST_TEST_CASE, fqcn, null, yamlTest);
    }

    private static PlanCommand byKey(List<PlanCommand> cmds, String key) {
        return cmds.stream().filter(c -> c.key().equals(key)).findFirst().orElseThrow();
    }
}
