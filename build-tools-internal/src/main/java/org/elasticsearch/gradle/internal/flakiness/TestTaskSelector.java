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
 * {@code Test}-task facts ({@link TestTaskInfo}), and NOT from the {@code :project:<sourceSet>} naming
 * convention.
 *
 * <p>The convention-free rule is a single query:
 * <blockquote>a target is run by the <b>enabled</b> {@code Test} tasks whose {@code testClassesDirs} overlap
 * the compiled-output directory of the source set that owns the class.</blockquote>
 *
 * <p>This matters because several ES conventions disable the bare task and point other {@code Test} tasks at
 * the same output:
 * <ul>
 *   <li>{@code elasticsearch.bwc-test} disables {@code test} and {@code javaRestTest} and points every
 *       {@code v<version>#bwcTest} {@code StandaloneRestIntegTestTask} at
 *       {@code sourceSets.javaRestTest.output.classesDirs};</li>
 *   <li>{@code elasticsearch.distro-test} ({@code qa/packaging}) disables {@code test} and points every
 *       {@code destructiveDistroTest.<distro>} task at the {@code test} source-set output.</li>
 * </ul>
 * Emitting the disabled bare task for those projects made Gradle report {@code SKIPPED}, run zero tests, exit
 * 0, and the analyzer record a bogus {@code hang}. Emitting the real tasks makes bwc tests genuinely re-runnable.
 */
public final class TestTaskSelector {

    /**
     * How many candidate tasks a single target may fan out to. A bwc project registers one
     * {@code v<version>#bwcTest} task per wire-compatible version - 67 of them for
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
     * as enabled and runnable. Note the reason is <em>not</em> host lifetime: the flakiness agent is a per-job
     * GCE VM exactly like the packaging one. They are excluded because:
     * <ul>
     *   <li><b>the image has to match.</b> {@code destructiveDistroTest.<distro>} installs a {@code .deb} or
     *       {@code .rpm} and asserts on systemd, so it assumes <em>the host is the distro under test</em>.
     *       Packaging CI therefore runs one job per OS image ({@code debian-12}, {@code rocky-8},
     *       {@code sles-15}, ...), while flakiness pins a single image; the {@code .docker*} variants also
     *       need prod registry credentials the flakiness steps do not set.</li>
     *   <li><b>ephemeral per job is not clean per iteration.</b> Flakiness re-runs each target N times within
     *       one job ({@code -Dtests.iters} for java targets, {@code repeat-rest-test.sh} for REST ones). A
     *       destructive test mutates the host on the first iteration - packages installed, users added,
     *       systemd units written - so the remaining iterations measure that contamination, not flakiness.
     *       A fresh VM per job does not help, because the repeats are inside the job.</li>
     * </ul>
     *
     * <p>The {@code destructive} task-name prefix is the ES-wide marker for exactly that host-mutating
     * property. On the local runner the host is the developer's own workstation, which AGENTS.md rules out
     * for packaging suites outright.
     */
    public static final String REASON_REQUIRES_PACKAGING_HOST = "requires-packaging-host";

    private static final String DESTRUCTIVE_TASK_PREFIX = "destructive";

    /**
     * Candidate tasks are ordered newest-first: a numeric-aware ("natural") comparison of the task name,
     * descending. For the {@code v<version>#bwcTest} family this yields the newest versions first and,
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
     * <h2>Known limitation: per-task class filters are not consulted</h2>
     * Selection is based on {@code testClassesDirs} overlap, which answers "could this task run classes from
     * that source set" but not "would it run <em>this</em> class". A {@code Test} task can also carry
     * {@code PatternFilterable} include/exclude patterns, and those are invisible here, so a class that the
     * chosen task excludes yields a command matching zero tests. Because
     * {@code MutedTestPlugin} sets {@code failOnNoMatchingTests(ci == false)}, that command <em>passes</em> on
     * CI having run nothing, and the batch is scored as a zero-test run - the false positive this selector
     * exists to prevent.
     *
     * <p>The shape that triggers it is a class excluded from the conventional task and included in a sibling
     * task pointed at the same output, which is how third-party and performance suites are kept out of the
     * normal run. Six classes across five projects have it today, among them
     * {@code S3RegisterCASLinearizabilityTests} ({@code :x-pack:plugin:stateless}) and
     * {@code AutomatonPatternsTests} ({@code :x-pack:plugin:core}); none are currently muted. Closing it
     * means snapshotting each task's pattern set into {@link TestTaskInfo} and rejecting tasks whose patterns
     * do not admit the requested class. Tracked as separate follow-up work.
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
        // Computed before the bare-task check so every path below reports the same denominator: the enabled,
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
