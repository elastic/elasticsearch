/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Decides <b>which Gradle task actually re-runs a resolved target</b>, from the project's real
 * {@code Test}-task facts ({@link TestTaskInfo}) rather than the {@code :project:&lt;sourceSet&gt;} naming
 * convention the pipeline used to assume.
 *
 * <p>The convention-free rule is a single query:
 * <blockquote>a target is run by the <b>enabled</b> {@code Test} tasks whose {@code testClassesDirs} overlap
 * the compiled-output directory of the source set that owns the class.</blockquote>
 *
 * <p>This matters because several ES conventions disable the bare task and point other {@code Test} tasks at
 * the same output:
 * <ul>
 *   <li>{@code elasticsearch.bwc-test} disables {@code test} and {@code javaRestTest} and points every
 *       {@code v&lt;version&gt;#bwcTest} {@code StandaloneRestIntegTestTask} at
 *       {@code sourceSets.javaRestTest.output.classesDirs};</li>
 *   <li>{@code elasticsearch.distro-test} ({@code qa/packaging}) disables {@code test} and points every
 *       {@code destructiveDistroTest.&lt;distro&gt;} task at the {@code test} source-set output.</li>
 * </ul>
 * Emitting the disabled bare task for those projects made Gradle report {@code SKIPPED}, run zero tests, exit
 * 0, and the analyzer record a bogus {@code hang}. Emitting the real tasks makes bwc tests genuinely
 * re-runnable for the first time.
 *
 * <p>Pure and Gradle-free, so all of it (overlap matching, the disposition decision, the cap and its
 * ordering, the packaging policy) is unit-testable without TestKit.
 */
public final class TestTaskSelector {

    /**
     * How many candidate tasks a single target may fan out to. A bwc project registers one
     * {@code v&lt;version&gt;#bwcTest} task per wire-compatible version - 67 of them for
     * {@code :x-pack:plugin:logsdb:qa:rolling-upgrade} at the time of writing - and each one boots a real
     * multi-node cluster, so an uncapped fan-out would swamp the pipeline. Overridable with
     * {@code -Pflakiness.taskCap}.
     */
    public static final int DEFAULT_TASK_CAP = 2;

    /** Skip reason: the source set has no enabled {@code Test} task at all, so there is nothing to re-run. */
    public static final String REASON_NO_RUNNABLE_TASK = "no-runnable-task";

    /**
     * Skip reason: the only tasks that would run this target are the {@code destructive*} packaging tests.
     *
     * <p><b>This is an agent-capability policy, not a model fact.</b> The model correctly reports those tasks
     * as enabled and runnable; they are excluded because they install/remove packages and mutate the host, so
     * they require a dedicated ephemeral packaging host (see AGENTS.md) that the standard flakiness agent is
     * not. The {@code destructive} task-name prefix is the ES-wide marker for exactly that property: the
     * {@code destructive*} tasks run against the local host, while their non-destructive wrappers delegate to
     * a throw-away VM.
     */
    public static final String REASON_REQUIRES_PACKAGING_HOST = "requires-packaging-host";

    private static final String DESTRUCTIVE_TASK_PREFIX = "destructive";

    /**
     * Candidate tasks are ordered newest-first: a numeric-aware ("natural") comparison of the task name,
     * descending. For the {@code v&lt;version&gt;#bwcTest} family this yields the newest versions first and,
     * unlike plain lexicographic ordering, orders {@code v8.19.10} above {@code v8.19.9}. Task names are
     * unique within a project, so the ordering is total - the capped selection is fully reproducible.
     */
    static final Comparator<TestTaskInfo> NEWEST_FIRST = Comparator.comparing(TestTaskInfo::name, TestTaskSelector::compareNatural)
        .reversed();

    private TestTaskSelector() {}

    /**
     * The chosen tasks for one target.
     *
     * @param taskPaths      the task paths to run, capped and newest-first; empty when {@code skipReason} is set
     * @param candidateCount how many enabled candidates existed before the cap (for the report)
     * @param skipReason     {@code null} when runnable, otherwise the precise reason the target cannot be run
     */
    public record Selection(List<String> taskPaths, int candidateCount, String skipReason) {

        public boolean runnable() {
            return skipReason == null;
        }
    }

    /**
     * Select the tasks that re-run a target.
     *
     * @param bareTaskName the conventional task name for the target's kind - which is always the source-set
     *                     name ({@code test}/{@code internalClusterTest}/{@code javaRestTest}/{@code yamlRestTest})
     * @param outputDir    the compiled-output directory of the owning source set
     * @param testTasks    every {@code Test} task of the owning project, post-configuration
     * @param cap          max tasks to select (see {@link #DEFAULT_TASK_CAP})
     */
    public static Selection select(String bareTaskName, Path outputDir, List<TestTaskInfo> testTasks, int cap) {
        List<TestTaskInfo> candidates = new ArrayList<>();
        for (TestTaskInfo t : testTasks) {
            if (t.enabled() && runsClassesIn(t, outputDir)) {
                candidates.add(t);
            }
        }
        if (candidates.isEmpty()) {
            return new Selection(List.of(), 0, REASON_NO_RUNNABLE_TASK);
        }

        // The bare conventional task, when it is enabled, remains the single canonical way to run the target -
        // today's behaviour, but now DERIVED from the model instead of assumed.
        // Computed before the bare-task check so every path below reports the SAME denominator: the enabled,
        // non-destructive tasks that run this output. Reporting `candidates.size()` on one path and
        // `runnableHere.size()` on another made "selected N of M" mean different things depending on which
        // branch produced it, and made PlanBuilder claim a capped fan-out where the bare task was simply
        // chosen canonically.
        List<TestTaskInfo> runnableHere = candidates.stream().filter(t -> isDestructive(t) == false).toList();

        for (TestTaskInfo t : candidates) {
            if (t.name().equals(bareTaskName)) {
                return new Selection(List.of(t.taskPath()), Math.max(runnableHere.size(), 1), null);
            }
        }

        // The bare task is disabled (or absent): fall back to the alternatives that really run this output.
        if (runnableHere.isEmpty()) {
            return new Selection(List.of(), candidates.size(), REASON_REQUIRES_PACKAGING_HOST);
        }
        List<String> selected = runnableHere.stream().sorted(NEWEST_FIRST).limit(Math.max(0, cap)).map(TestTaskInfo::taskPath).toList();
        if (selected.isEmpty()) {
            // A cap of zero disables the fan-out entirely; say so rather than emit an empty run entry.
            return new Selection(List.of(), runnableHere.size(), REASON_NO_RUNNABLE_TASK);
        }
        return new Selection(selected, runnableHere.size(), null);
    }

    /**
     * Whether a task runs the classes compiled into {@code outputDir}. Compared as normalized absolute paths:
     * a {@code Test} task's {@code testClassesDirs} are the source-set output's {@code classesDirs}, of which
     * the source set's java output directory is one element, so exact membership is the right test.
     */
    static boolean runsClassesIn(TestTaskInfo task, Path outputDir) {
        if (outputDir == null) {
            return false;
        }
        Path target = outputDir.toAbsolutePath().normalize();
        for (Path dir : task.testClassesDirs()) {
            if (dir.toAbsolutePath().normalize().equals(target)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDestructive(TestTaskInfo task) {
        return task.name().startsWith(DESTRUCTIVE_TASK_PREFIX);
    }

    /**
     * Compare two task names treating digit runs as numbers, so {@code v8.19.10} sorts above {@code v8.19.9}.
     * Digit runs compare by value (any length, so no overflow), everything else compares by code point.
     */
    static int compareNatural(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            char ca = a.charAt(i);
            char cb = b.charAt(j);
            if (Character.isDigit(ca) && Character.isDigit(cb)) {
                int endA = digitRunEnd(a, i);
                int endB = digitRunEnd(b, j);
                int cmp = compareDigitRuns(a.substring(i, endA), b.substring(j, endB));
                if (cmp != 0) {
                    return cmp;
                }
                i = endA;
                j = endB;
            } else {
                if (ca != cb) {
                    return Character.compare(ca, cb);
                }
                i++;
                j++;
            }
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    private static int digitRunEnd(String s, int from) {
        int i = from;
        while (i < s.length() && Character.isDigit(s.charAt(i))) {
            i++;
        }
        return i;
    }

    private static int compareDigitRuns(String a, String b) {
        String sa = stripLeadingZeros(a);
        String sb = stripLeadingZeros(b);
        if (sa.length() != sb.length()) {
            return Integer.compare(sa.length(), sb.length());
        }
        int cmp = sa.compareTo(sb);
        // Equal numeric value: the shorter (less zero-padded) spelling sorts first, keeping the order total.
        return cmp != 0 ? cmp : Integer.compare(a.length(), b.length());
    }

    private static String stripLeadingZeros(String digits) {
        int i = 0;
        while (i < digits.length() - 1 && digits.charAt(i) == '0') {
            i++;
        }
        return digits.substring(i);
    }
}
