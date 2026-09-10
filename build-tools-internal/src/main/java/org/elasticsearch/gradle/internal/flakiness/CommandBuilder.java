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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Turns the concrete, runnable {@link PlanEntry}s of a {@link FlakinessPlan} into the ready batch commands
 * ({@link PlanCommand}) carried in {@code flakiness-plan.json}.
 *
 * <p>Every emitted command uses the {@link PlanCommand#GRADLE_PLACEHOLDER} token where the gradle binary
 * belongs, keeping the plan target-neutral (the runner layer substitutes {@code .ci/scripts/run-gradle.sh}
 * or {@code ./gradlew}).
 *
 * <h2>Task paths come from the plan, not from a convention</h2>
 * The invocation is built from each entry's {@code runnableTasks} (authoritative task paths from the project's
 * real {@code Test} tasks), never from an assumed {@code :project:<kind>}. The batching unit is therefore
 * an (entry, task path) pair: an entry with several runnable tasks - the capped
 * {@code v<version>#bwcTest} set of a bwc project - contributes one unit per task. Units are then batched
 * per kind, so the per-kind cap bounds real gradle task invocations and the
 * one-task-per-step kinds (javaRestTest, cap 1) keep one bwc version per Buildkite job - clean per-task
 * attribution for the analyzer.
 */
public final class CommandBuilder {

    /** The REST-loop wrapper: repeats a gradle invocation {@code restIters} times, preserving each run's XML. */
    static final String REPEAT_REST = ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh";

    private static final String G = PlanCommand.GRADLE_PLACEHOLDER;

    /** Iteration counts + suite timeout, with the {@code FLAKINESS_ITERS} override applied. */
    public record Config(int testIters, int internalClusterTestIters, int restIters, long suiteTimeoutMs) {

        public static final int DEFAULT_TEST_ITERS = 100;
        public static final int DEFAULT_INTERNAL_CLUSTER_TEST_ITERS = 20;
        public static final int DEFAULT_REST_ITERS = 10;
        public static final long DEFAULT_SUITE_TIMEOUT_MS = 3_600_000L;

        public static Config defaults() {
            return new Config(DEFAULT_TEST_ITERS, DEFAULT_INTERNAL_CLUSTER_TEST_ITERS, DEFAULT_REST_ITERS, DEFAULT_SUITE_TIMEOUT_MS);
        }

        /**
         * Apply the operator's {@code FLAKINESS_ITERS} (or {@code -Pflakiness.iters}) override: a positive
         * value sets the unit, internalClusterTest, and REST iteration counts uniformly (matching the old
         * manual/local behaviour). Non-positive/absent leaves the defaults untouched.
         */
        public Config withIterOverride(Integer iters) {
            if (iters == null || iters <= 0) {
                return this;
            }
            return new Config(iters, iters, iters, suiteTimeoutMs);
        }
    }

    private CommandBuilder() {}

    /**
     * One unit of work: a plan entry paired with one of its runnable task paths. Entries with several runnable
     * tasks (a capped bwc fan-out) explode into one unit per task, so batching and the emitted gradle
     * command line are both expressed purely in real task paths.
     */
    private record RunUnit(PlanEntry entry, String taskPath) {}

    /** Build the batch commands for the run entries, in {@link Kinds#KIND_ORDER}, capped per kind. */
    public static List<PlanCommand> build(List<PlanEntry> runEntries, Config cfg) {
        List<PlanEntry> staged = deduplicateYamlRunners(collapseYamlSuites(dedupe(runEntries)));

        Map<String, List<RunUnit>> byKind = new LinkedHashMap<>();
        for (PlanEntry e : staged) {
            for (String taskPath : e.runnableTasks()) {
                byKind.computeIfAbsent(e.kind(), k -> new ArrayList<>()).add(new RunUnit(e, taskPath));
            }
        }

        List<PlanCommand> out = new ArrayList<>();
        for (String kind : Kinds.KIND_ORDER) {
            List<RunUnit> kindUnits = byKind.get(kind);
            if (kindUnits == null) {
                continue;
            }
            int cap = Kinds.KIND_CAP.get(kind);
            for (int i = 0; i < kindUnits.size(); i += cap) {
                List<RunUnit> batch = kindUnits.subList(i, Math.min(i + cap, kindUnits.size()));
                out.add(
                    new PlanCommand(
                        kind,
                        Kinds.KIND_LABEL.get(kind),
                        Kinds.KIND_KEY.get(kind),
                        batchCommand(batch, cfg),
                        taskPathsOf(batch)
                    )
                );
            }
        }
        return out;
    }

    private static List<PlanEntry> dedupe(List<PlanEntry> tests) {
        Map<String, PlanEntry> seen = new LinkedHashMap<>();
        for (PlanEntry t : tests) {
            // Note what this does NOT collapse: two yaml runners in one project differ here as soon as they
            // carry an fqcn, which expanded/re-homed runners do. Collapsing those is
            // deduplicateYamlRunners' job, and it has to stay a separate pass for that reason.
            String identity = t.yamlTest() != null ? t.yamlTest()
                : t.fqcn() != null ? t.fqcn()
                : t.suitePath() != null ? t.suitePath()
                : "";
            seen.putIfAbsent(t.gradleProject() + "|" + t.kind() + "|" + identity, t);
        }
        return new ArrayList<>(seen.values());
    }

    /**
     * Collapse multiple yaml suite files that live in the same directory (of the same project) into a single
     * directory-level target; suites alone in a directory (or at the root) keep their individual paths.
     */
    private static List<PlanEntry> collapseYamlSuites(List<PlanEntry> tests) {
        Map<String, List<PlanEntry>> suitesByProject = new LinkedHashMap<>();
        List<PlanEntry> result = new ArrayList<>();
        // first group by project
        for (PlanEntry t : tests) {
            if (t.kind().equals(Kinds.YAML_REST_TEST_SUITE)) {
                suitesByProject.computeIfAbsent(t.gradleProject(), k -> new ArrayList<>()).add(t);
            } else {
                result.add(t);
            }
        }
        for (List<PlanEntry> suites : suitesByProject.values()) {
            Map<String, List<PlanEntry>> byDir = new LinkedHashMap<>();
            // then group by directory within each project
            for (PlanEntry suite : suites) {
                byDir.computeIfAbsent(dirname(suite.suitePath()), k -> new ArrayList<>()).add(suite);
            }
            // then collapse each directory's suites into a single directory-level target
            for (Map.Entry<String, List<PlanEntry>> e : byDir.entrySet()) {
                String dir = e.getKey();
                List<PlanEntry> dirSuites = e.getValue();
                if (dirSuites.size() > 1 && dir.equals(".") == false) {
                    PlanEntry first = dirSuites.getFirst();
                    result.add(
                        new PlanEntry(
                            first.gradleProject(),
                            Kinds.SS_YAML_REST_TEST,
                            Kinds.YAML_REST_TEST_SUITE,
                            null,
                            dir,
                            null,
                            Kinds.DISPOSITION_RUN,
                            null,
                            null,
                            first.runnableTasks()
                        )
                    );
                } else {
                    result.addAll(dirSuites);
                }
            }
        }
        return result;
    }

    /**
     * Keep only the first yaml runner per project: the runner command ignores {@code fqcn} and re-runs the
     * whole source set, so a second entry for the same project would emit a byte-identical command.
     *
     * <p>This is <em>not</em> covered by {@link #dedupe} above, even though that collapses same-project
     * runners whose identity is empty. An expanded or re-homed runner carries an {@code fqcn} - a concrete
     * subclass found in some project's {@code yamlRestTest} output, re-homed onto that project's runner task
     * - so {@code dedupe} sees distinct identities and keeps every one. Without this pass, an abstract base
     * with three yaml-runner subclasses in one project emits three identical commands, each starting its own
     * test cluster and running the same suite.
     */
    private static List<PlanEntry> deduplicateYamlRunners(List<PlanEntry> tests) {
        Set<String> seen = new LinkedHashSet<>();
        List<PlanEntry> result = new ArrayList<>();
        for (PlanEntry t : tests) {
            if (t.kind().equals(Kinds.YAML_REST_TEST_RUNNER)) {
                if (seen.add(t.gradleProject()) == false) {
                    continue;
                }
            }
            result.add(t);
        }
        return result;
    }

    private static String batchCommand(List<RunUnit> batch, Config cfg) {
        String kind = batch.get(0).entry().kind();
        return switch (kind) {
            case Kinds.TEST -> G
                + " -Dtests.iters="
                + cfg.testIters()
                + " -Dtests.timeoutSuite="
                + cfg.suiteTimeoutMs()
                + "! "
                + tasksWithFilters(batch, t -> "--tests " + t.fqcn(), null);
            case Kinds.INTERNAL_CLUSTER_TEST -> G
                + " -Dtests.iters="
                + cfg.internalClusterTestIters()
                + " -Dtests.timeoutSuite="
                + cfg.suiteTimeoutMs()
                + "! "
                + tasksWithFilters(batch, t -> "--tests " + t.fqcn(), null);
            case Kinds.JAVA_REST_TEST -> REPEAT_REST
                + " "
                + cfg.restIters()
                + " "
                + G
                + " "
                + tasksWithFilters(batch, t -> "--tests " + t.fqcn(), "--rerun");
            case Kinds.YAML_REST_TEST_RUNNER -> REPEAT_REST + " " + cfg.restIters() + " " + G + " " + batch.get(0).taskPath() + " --rerun";
            case Kinds.YAML_REST_TEST_SUITE -> yamlSuiteCommand(batch, cfg);
            case Kinds.YAML_REST_TEST_CASE -> REPEAT_REST
                + " "
                + cfg.restIters()
                + " "
                + G
                + " "
                + tasksWithFilters(batch, t -> "--tests \"" + t.fqcn() + "." + t.yamlTest() + "\"", "--rerun");
            default -> throw new IllegalStateException("unexpected batch kind: " + kind);
        };
    }

    /**
     * {@code tests.rest.suite} is a JVM system property, not a Gradle task option, so a single value would
     * apply to every yamlRestTest task in the invocation. ESClientYamlSuiteTestCase recognises a per-task
     * scoped variant {@code tests.rest.suite.<task path>} so each task gets only its own suites.
     */
    private static String yamlSuiteCommand(List<RunUnit> batch, Config cfg) {
        Map<String, List<String>> byTask = new LinkedHashMap<>();
        for (RunUnit u : batch) {
            byTask.computeIfAbsent(u.taskPath(), k -> new ArrayList<>()).add(u.entry().suitePath());
        }
        String tasks = byTask.keySet().stream().map(task -> task + " --rerun").collect(Collectors.joining(" "));
        String suiteProps = byTask.entrySet()
            .stream()
            .map(e -> "-Dtests.rest.suite." + e.getKey() + "=" + String.join(",", e.getValue()))
            .collect(Collectors.joining(" "));
        return REPEAT_REST + " " + cfg.restIters() + " " + G + " " + tasks + " " + suiteProps;
    }

    /**
     * The distinct task paths a batch invokes, in first-seen order - the same order and grouping
     * {@link #tasksWithFilters} emits into the command, so the two never disagree.
     */
    private static List<String> taskPathsOf(List<RunUnit> batch) {
        return batch.stream().map(RunUnit::taskPath).distinct().toList();
    }

    private static String tasksWithFilters(List<RunUnit> batch, Function<PlanEntry, String> toFilter, String perTaskSuffix) {
        Map<String, List<String>> byTask = new LinkedHashMap<>();
        for (RunUnit u : batch) {
            byTask.computeIfAbsent(u.taskPath(), k -> new ArrayList<>()).add(toFilter.apply(u.entry()));
        }
        return byTask.entrySet().stream().map(e -> {
            List<String> parts = new ArrayList<>();
            parts.add(e.getKey());
            parts.addAll(e.getValue());
            if (perTaskSuffix != null) {
                parts.add(perTaskSuffix);
            }
            return String.join(" ", parts);
        }).collect(Collectors.joining(" "));
    }

    /**
     * The parent dir, or {@code "."} when there is no slash.
     */
    private static String dirname(String path) {
        Path parent = Paths.get(path).getParent();
        return parent == null ? "." : parent.toString();
    }
}
