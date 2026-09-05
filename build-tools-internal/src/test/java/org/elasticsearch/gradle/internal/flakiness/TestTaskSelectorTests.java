/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the pure task-selection core: output-dir overlap matching, the disposition decision (bare
 * task / alternatives / nothing runnable), the deterministic newest-first cap, and the packaging-host policy.
 * All of it is Gradle-free, so no TestKit is involved; the lifecycle half (that the {@code enabled} /
 * {@code testClassesDirs} values fed in here are post-configuration ones) is proved by the real-build run
 * recorded in JAVA_RESOLVER_NOTES.md and by {@code FlakinessResolvePluginFuncTest}.
 */
public class TestTaskSelectorTests {

    private static final Path TEST_OUT = Path.of("/repo/proj/build/classes/java/test");
    private static final Path REST_OUT = Path.of("/repo/proj/build/classes/java/javaRestTest");

    @Test
    public void testEnabledBareTaskIsCanonical() {
        List<TestTaskInfo> tasks = List.of(task("test", true, TEST_OUT), task("someOtherTest", true, TEST_OUT));

        TestTaskSelector.Selection s = TestTaskSelector.select("test", TEST_OUT, tasks, 2);

        assertThat(s.runnable(), is(true));
        assertThat(s.skipReason(), is(nullValue()));
        assertThat(s.taskPaths(), contains(":proj:test"));
        // Honest reporting: the other candidate existed, we deliberately chose the conventional task.
        assertThat(s.candidateCount(), equalTo(2));
    }

    @Test
    public void testOnlyTasksWhoseClassesDirsOverlapAreCandidates() {
        List<TestTaskInfo> tasks = List.of(
            // Right name, wrong output: a task that does not run these classes is not a candidate.
            task("test", true, REST_OUT),
            task("javaRestTest", true, REST_OUT)
        );

        TestTaskSelector.Selection s = TestTaskSelector.select("test", TEST_OUT, tasks, 2);

        assertThat(s.runnable(), is(false));
        assertThat(s.skipReason(), equalTo(TestTaskSelector.REASON_NO_RUNNABLE_TASK));
        assertThat(s.taskPaths(), is(empty()));
    }

    @Test
    public void testOverlapMatchesOneOfSeveralClassesDirs() {
        // A Test task typically runs a source set's whole `output.classesDirs`, of which the java output is
        // one element - so membership, not equality of the whole set, is what matters.
        TestTaskInfo multi = new TestTaskInfo(
            "someTest",
            ":proj:someTest",
            true,
            List.of(Path.of("/repo/proj/build/classes/groovy/test"), TEST_OUT)
        );
        assertThat(TestTaskSelector.runsClassesIn(multi, TEST_OUT), is(true));
        // Non-normalized paths still match.
        assertThat(TestTaskSelector.runsClassesIn(multi, Path.of("/repo/proj/build/classes/java/../java/test")), is(true));
        assertThat(TestTaskSelector.runsClassesIn(multi, REST_OUT), is(false));
    }

    /** The bwc shape: bare task disabled, differently named tasks pointed at the same output. */
    @Test
    public void testDisabledBareTaskFallsBackToAlternatives() {
        List<TestTaskInfo> tasks = List.of(
            task("javaRestTest", false, REST_OUT),
            task("bcUpgradeTest", true, REST_OUT),
            task("v9.5.1#bwcTest", true, REST_OUT),
            task("v9.6.0#bwcTest", true, REST_OUT)
        );

        TestTaskSelector.Selection s = TestTaskSelector.select("javaRestTest", REST_OUT, tasks, 2);

        assertThat(s.runnable(), is(true));
        assertThat(s.taskPaths(), contains(":proj:v9.6.0#bwcTest", ":proj:v9.5.1#bwcTest"));
        assertThat(s.candidateCount(), equalTo(3));
    }

    @Test
    public void testCapIsDeterministicAndPrefersNewestVersions() {
        List<TestTaskInfo> tasks = new ArrayList<>(List.of(task("javaRestTest", false, REST_OUT)));
        // Deliberately registered out of order, and with versions where lexicographic ordering is wrong.
        for (String v : List.of("8.19.2", "9.6.0", "8.19.10", "9.5.1", "10.0.0", "8.19.9")) {
            tasks.add(task("v" + v + "#bwcTest", true, REST_OUT));
        }

        TestTaskSelector.Selection s = TestTaskSelector.select("javaRestTest", REST_OUT, tasks, 3);

        assertThat(s.taskPaths(), contains(":proj:v10.0.0#bwcTest", ":proj:v9.6.0#bwcTest", ":proj:v9.5.1#bwcTest"));
        assertThat(s.candidateCount(), equalTo(6));
        // Reordering the input does not change the selection.
        List<TestTaskInfo> reversed = new ArrayList<>(tasks);
        Collections.reverse(reversed);
        assertThat(TestTaskSelector.select("javaRestTest", REST_OUT, reversed, 3).taskPaths(), equalTo(s.taskPaths()));
    }

    @Test
    public void testNaturalOrderingComparesDigitRunsNumerically() {
        assertThat(TestTaskSelector.compareNatural("v8.19.9#bwcTest", "v8.19.10#bwcTest"), is(lessThan(0)));
        assertThat(TestTaskSelector.compareNatural("v9.6.0#bwcTest", "v10.0.0#bwcTest"), is(lessThan(0)));
        assertThat(TestTaskSelector.compareNatural("bcUpgradeTest", "v9.6.0#bwcTest"), is(lessThan(0)));
        assertThat(TestTaskSelector.compareNatural("v9.6.0#bwcTest", "v9.6.0#bwcTest"), is(0));
    }

    /**
     * The packaging policy: {@code destructive*} tasks are the only thing that would run these classes, but
     * they mutate the host, so the target is not runnable on the flakiness agent. This is an agent-capability
     * decision, not a model fact - the model reports them as perfectly runnable.
     */
    @Test
    public void testDestructivePackagingTasksAreNotRunnableHere() {
        List<TestTaskInfo> tasks = List.of(
            task("test", false, TEST_OUT),
            task("destructiveDistroTest.default-deb", true, TEST_OUT),
            task("destructiveDistroUpgradeTest.v8.0.0.default-rpm", true, TEST_OUT)
        );

        TestTaskSelector.Selection s = TestTaskSelector.select("test", TEST_OUT, tasks, 2);

        assertThat(s.runnable(), is(false));
        assertThat(s.skipReason(), equalTo(TestTaskSelector.REASON_REQUIRES_PACKAGING_HOST));
        assertThat(s.taskPaths(), is(empty()));
        // The candidate count is still reported honestly, so the report can say what was rejected.
        assertThat(s.candidateCount(), equalTo(2));
    }

    @Test
    public void testNonDestructiveAlternativeWinsOverDestructiveOnes() {
        List<TestTaskInfo> tasks = List.of(
            task("test", false, TEST_OUT),
            task("destructiveDistroTest.default-deb", true, TEST_OUT),
            task("integTest", true, TEST_OUT)
        );

        TestTaskSelector.Selection s = TestTaskSelector.select("test", TEST_OUT, tasks, 2);

        assertThat(s.runnable(), is(true));
        assertThat(s.taskPaths(), contains(":proj:integTest"));
    }

    @Test
    public void testNoTestTasksAtAll() {
        TestTaskSelector.Selection s = TestTaskSelector.select("test", TEST_OUT, List.of(), 2);
        assertThat(s.skipReason(), equalTo(TestTaskSelector.REASON_NO_RUNNABLE_TASK));
        assertThat(s.candidateCount(), equalTo(0));
    }

    @Test
    public void testAllCandidatesDisabled() {
        List<TestTaskInfo> tasks = List.of(task("test", false, TEST_OUT), task("v9.6.0#bwcTest", false, TEST_OUT));
        TestTaskSelector.Selection s = TestTaskSelector.select("test", TEST_OUT, tasks, 2);
        assertThat(s.skipReason(), equalTo(TestTaskSelector.REASON_NO_RUNNABLE_TASK));
    }

    @Test
    public void testZeroCapReportsNothingRunnableRatherThanAnEmptyRun() {
        List<TestTaskInfo> tasks = List.of(task("javaRestTest", false, REST_OUT), task("v9.6.0#bwcTest", true, REST_OUT));
        TestTaskSelector.Selection s = TestTaskSelector.select("javaRestTest", REST_OUT, tasks, 0);
        assertThat(s.runnable(), is(false));
        assertThat(s.skipReason(), equalTo(TestTaskSelector.REASON_NO_RUNNABLE_TASK));
    }

    private static TestTaskInfo task(String name, boolean enabled, Path classesDir) {
        return new TestTaskInfo(name, ":proj:" + name, enabled, List.of(classesDir));
    }
}
