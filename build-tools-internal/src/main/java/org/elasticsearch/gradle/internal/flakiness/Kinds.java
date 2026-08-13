/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wire-level test-kind and source-set constants shared between the Java resolver and the TypeScript
 * orchestration layer. The strings are a hard contract: they appear verbatim in {@code flakiness-plan.json}
 * (contract 2) and are consumed by {@code commands.ts}. Keep them in sync with {@code domain.ts}'s
 * {@code TestKind}.
 */
public final class Kinds {

    private Kinds() {}

    // Source-set names (Gradle sourceSet names). yamlRestTest fans out into three kinds below.
    public static final String SS_TEST = "test";
    public static final String SS_INTERNAL_CLUSTER_TEST = "internalClusterTest";
    public static final String SS_JAVA_REST_TEST = "javaRestTest";
    public static final String SS_YAML_REST_TEST = "yamlRestTest";

    // Test kinds (the {@code kind} field in a plan entry).
    public static final String TEST = "test";
    public static final String INTERNAL_CLUSTER_TEST = "internalClusterTest";
    public static final String JAVA_REST_TEST = "javaRestTest";
    public static final String YAML_REST_TEST_SUITE = "yamlRestTestSuite";
    public static final String YAML_REST_TEST_RUNNER = "yamlRestTestRunner";
    public static final String YAML_REST_TEST_CASE = "yamlRestTestCase";

    // Dispositions.
    public static final String DISPOSITION_RUN = "run";
    public static final String DISPOSITION_SKIP = "skip";

    /**
     * The Java-backed kinds that carry an FQCN and therefore participate in bytecode enrichment
     * (abstract detection + subclass expansion). The yaml kinds either have no class or address a
     * specific parameterised case, so they pass through untouched.
     */
    public static final Set<String> BYTECODE_ENRICHED = Set.of(TEST, INTERNAL_CLUSTER_TEST, JAVA_REST_TEST);

    // ---------------------------------------------------------------------------
    // Batching wire contract, shared verbatim with domain.ts (KIND_ORDER / KIND_LABELS / KIND_KEYS /
    // BATCH_CAPS). The Java resolver now owns batch-command generation (see CommandBuilder), so these must
    // stay in sync with the TypeScript tables that still key the analyze step and the pipeline grouping.
    // ---------------------------------------------------------------------------

    /** Deterministic emit order of batch steps by kind. */
    public static final List<String> KIND_ORDER = List.of(
        TEST,
        INTERNAL_CLUSTER_TEST,
        JAVA_REST_TEST,
        YAML_REST_TEST_RUNNER,
        YAML_REST_TEST_SUITE,
        YAML_REST_TEST_CASE
    );

    /** Human label for each kind's Buildkite step. */
    public static final Map<String, String> KIND_LABEL = Map.of(
        TEST,
        "unit tests",
        INTERNAL_CLUSTER_TEST,
        "integ tests",
        JAVA_REST_TEST,
        "java rest tests",
        YAML_REST_TEST_RUNNER,
        "yaml rest test runner",
        YAML_REST_TEST_SUITE,
        "yaml rest tests",
        YAML_REST_TEST_CASE,
        "yaml rest test cases"
    );

    /** Buildkite step key for each kind. */
    public static final Map<String, String> KIND_KEY = Map.of(
        TEST,
        "flakiness-detection:unit",
        INTERNAL_CLUSTER_TEST,
        "flakiness-detection:integ",
        JAVA_REST_TEST,
        "flakiness-detection:java-rest",
        YAML_REST_TEST_RUNNER,
        "flakiness-detection:yaml-runner",
        YAML_REST_TEST_SUITE,
        "flakiness-detection:yaml-suite",
        YAML_REST_TEST_CASE,
        "flakiness-detection:yaml-case"
    );

    /** Max tests batched into one Buildkite job per kind, sized from p95 durations * iteration counts. */
    public static final Map<String, Integer> KIND_CAP = Map.of(
        TEST,
        3,
        INTERNAL_CLUSTER_TEST,
        2,
        JAVA_REST_TEST,
        1,
        YAML_REST_TEST_RUNNER,
        1,
        YAML_REST_TEST_SUITE,
        4,
        YAML_REST_TEST_CASE,
        4
    );
}
