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
 * ({@link PlanCommand}) carried in {@code flakiness-plan.json}. This is the authoritative port of the old
 * TypeScript {@code commands.ts} (dedupe / collapse-yaml-suites / dedup-runners / cap-batching /
 * per-kind command generation), so the TS {@code generate} step no longer batches - it only maps these to
 * Buildkite steps.
 *
 * <p>Every emitted command uses the {@link PlanCommand#GRADLE_PLACEHOLDER} token where the gradle binary
 * belongs, keeping the plan target-neutral (the runner layer substitutes {@code .ci/scripts/run-gradle.sh}
 * or {@code ./gradlew}). Pure and Gradle-free, so it is unit-testable without TestKit.
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

    /** Build the batch commands for the run entries, in {@link Kinds#KIND_ORDER}, capped per kind. */
    public static List<PlanCommand> build(List<PlanEntry> runEntries, Config cfg) {
        List<PlanEntry> staged = deduplicateYamlRunners(collapseYamlSuites(dedupe(runEntries)));

        Map<String, List<PlanEntry>> byKind = new LinkedHashMap<>();
        for (PlanEntry e : staged) {
            byKind.computeIfAbsent(e.kind(), k -> new ArrayList<>()).add(e);
        }

        List<PlanCommand> out = new ArrayList<>();
        for (String kind : Kinds.KIND_ORDER) {
            List<PlanEntry> kindTests = byKind.get(kind);
            if (kindTests == null) {
                continue;
            }
            int cap = Kinds.KIND_CAP.get(kind);
            for (int i = 0; i < kindTests.size(); i += cap) {
                List<PlanEntry> batch = kindTests.subList(i, Math.min(i + cap, kindTests.size()));
                out.add(new PlanCommand(kind, Kinds.KIND_LABEL.get(kind), Kinds.KIND_KEY.get(kind), batchCommand(batch, cfg)));
            }
        }
        return out;
    }

    private static List<PlanEntry> dedupe(List<PlanEntry> tests) {
        Map<String, PlanEntry> seen = new LinkedHashMap<>();
        for (PlanEntry t : tests) {
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
        for (PlanEntry t : tests) {
            if (t.kind().equals(Kinds.YAML_REST_TEST_SUITE)) {
                suitesByProject.computeIfAbsent(t.gradleProject(), k -> new ArrayList<>()).add(t);
            } else {
                result.add(t);
            }
        }
        for (List<PlanEntry> suites : suitesByProject.values()) {
            Map<String, List<PlanEntry>> byDir = new LinkedHashMap<>();
            for (PlanEntry suite : suites) {
                byDir.computeIfAbsent(dirname(suite.suitePath()), k -> new ArrayList<>()).add(suite);
            }
            for (Map.Entry<String, List<PlanEntry>> e : byDir.entrySet()) {
                String dir = e.getKey();
                List<PlanEntry> dirSuites = e.getValue();
                if (dirSuites.size() > 1 && dir.equals(".") == false) {
                    PlanEntry first = dirSuites.get(0);
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
                            null
                        )
                    );
                } else {
                    result.addAll(dirSuites);
                }
            }
        }
        return result;
    }

    /** Keep only the first yaml runner per project (running the whole source set once is enough). */
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

    private static String batchCommand(List<PlanEntry> batch, Config cfg) {
        String kind = batch.get(0).kind();
        return switch (kind) {
            case Kinds.TEST -> G
                + " -Dtests.iters="
                + cfg.testIters()
                + " -Dtests.timeoutSuite="
                + cfg.suiteTimeoutMs()
                + "! "
                + tasksWithFilters(batch, "test", t -> "--tests " + t.fqcn(), null);
            case Kinds.INTERNAL_CLUSTER_TEST -> G
                + " -Dtests.iters="
                + cfg.internalClusterTestIters()
                + " -Dtests.timeoutSuite="
                + cfg.suiteTimeoutMs()
                + "! "
                + tasksWithFilters(batch, "internalClusterTest", t -> "--tests " + t.fqcn(), null);
            case Kinds.JAVA_REST_TEST -> REPEAT_REST
                + " "
                + cfg.restIters()
                + " "
                + G
                + " "
                + tasksWithFilters(batch, "javaRestTest", t -> "--tests " + t.fqcn(), "--rerun");
            case Kinds.YAML_REST_TEST_RUNNER -> REPEAT_REST
                + " "
                + cfg.restIters()
                + " "
                + G
                + " "
                + batch.get(0).gradleProject()
                + ":yamlRestTest --rerun";
            case Kinds.YAML_REST_TEST_SUITE -> yamlSuiteCommand(batch, cfg);
            case Kinds.YAML_REST_TEST_CASE -> REPEAT_REST
                + " "
                + cfg.restIters()
                + " "
                + G
                + " "
                + tasksWithFilters(batch, "yamlRestTest", t -> "--tests \"" + t.fqcn() + "." + t.yamlTest() + "\"", "--rerun");
            default -> throw new IllegalStateException("unexpected batch kind: " + kind);
        };
    }

    /**
     * {@code tests.rest.suite} is a JVM system property, not a Gradle task option, so a single value would
     * apply to every yamlRestTest task in the invocation. ESClientYamlSuiteTestCase recognises a per-task
     * scoped variant {@code tests.rest.suite.<task path>} so each task gets only its own suites.
     */
    private static String yamlSuiteCommand(List<PlanEntry> batch, Config cfg) {
        Map<String, List<String>> byTask = new LinkedHashMap<>();
        for (PlanEntry t : batch) {
            byTask.computeIfAbsent(t.gradleProject() + ":yamlRestTest", k -> new ArrayList<>()).add(t.suitePath());
        }
        String tasks = byTask.keySet().stream().map(task -> task + " --rerun").collect(Collectors.joining(" "));
        String suiteProps = byTask.entrySet()
            .stream()
            .map(e -> "-Dtests.rest.suite." + e.getKey() + "=" + String.join(",", e.getValue()))
            .collect(Collectors.joining(" "));
        return REPEAT_REST + " " + cfg.restIters() + " " + G + " " + tasks + " " + suiteProps;
    }

    /**
     * Gradle task-level options ({@code --tests}, {@code --rerun}) bind to the most recently named task on
     * the command line, so per-task options must follow each {@code :project:taskName} they apply to.
     */
    private static String tasksWithFilters(
        List<PlanEntry> batch,
        String taskName,
        Function<PlanEntry, String> toFilter,
        String perTaskSuffix
    ) {
        Map<String, List<String>> byTask = new LinkedHashMap<>();
        for (PlanEntry t : batch) {
            byTask.computeIfAbsent(t.gradleProject() + ":" + taskName, k -> new ArrayList<>()).add(toFilter.apply(t));
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

    /** Mirrors Node's {@code path.dirname}: the parent dir, or {@code "."} when there is no slash. */
    private static String dirname(String path) {
        int idx = path.lastIndexOf('/');
        return idx < 0 ? "." : path.substring(0, idx);
    }
}
