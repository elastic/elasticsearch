/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Which CI part ({@code checkPart1..N}) runs each test task, and which part runs the rest of a
 * project's {@code check} (precommit and other non-test verification).
 *
 * <p>The assignment is generated from historical durations by {@link CiPartPlanner} and checked in, so
 * the placement of every task is reviewable and reproducible. Anything it does not list (a new test
 * task, a new project) falls back to a stable hash of its path, so the assignment never has to be
 * regenerated for the build to stay correct - only to stay balanced.
 *
 * @param parts    the number of CI parts; part numbers are {@code 1..parts}
 * @param splits   test tasks that run as several shards, keyed by task path
 * @param tests    test task path (or {@code <task path>#<shard>} for a split task) to part
 * @param projects project path to the part that runs its non-test {@code check} work
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({ "comment", "parts", "splits", "tests", "projects" })
public record CiPartAssignment(
    @JsonProperty("parts") int parts,
    @JsonProperty("splits") Map<String, Split> splits,
    @JsonProperty("tests") Map<String, Integer> tests,
    @JsonProperty("projects") Map<String, Integer> projects
) {

    static final String COMMENT =
        "Generated from config.json and durations.json in this directory, see .ci/test-parts/README.md. Do not edit.";

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    /**
     * How a test task is split into shards.
     *
     * @param count the number of shards
     * @param by    what is distributed across shards
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Split(@JsonProperty("count") int count, @JsonProperty("by") SplitBy by) {
        public Split {
            if (count < 2) {
                throw new IllegalArgumentException("a split needs at least 2 shards but was " + count);
            }
            if (by == null) {
                throw new IllegalArgumentException("a split needs a 'by' mode");
            }
        }
    }

    /** What a split test task distributes across its shards. */
    public enum SplitBy {
        /** Top-level test classes: for tasks with many classes. */
        @JsonProperty("class")
        CLASS,
        /** YAML test files of a single {@code ESClientYamlSuiteTestCase} class. */
        @JsonProperty("yamlFile")
        YAML_FILE
    }

    public CiPartAssignment {
        if (parts < 1) {
            throw new IllegalArgumentException("parts must be positive but was " + parts);
        }
        splits = new TreeMap<>(splits == null ? Map.of() : splits);
        tests = new TreeMap<>(tests == null ? Map.of() : tests);
        projects = new TreeMap<>(projects == null ? Map.of() : projects);
        for (Map<String, Integer> m : List.of(tests, projects)) {
            for (Map.Entry<String, Integer> e : m.entrySet()) {
                if (e.getValue() == null || e.getValue() < 1 || e.getValue() > parts) {
                    throw new IllegalArgumentException("[" + e.getKey() + "] is assigned to part " + e.getValue() + " of " + parts);
                }
            }
        }
    }

    @JsonProperty("comment")
    String comment() {
        return COMMENT;
    }

    /** The part running test task {@code taskPath}, or a hash-based default for a task the assignment does not know. */
    public int partOfTest(String taskPath) {
        return tests.getOrDefault(taskPath, defaultPart(taskPath));
    }

    /** The part running shard {@code shard} of split test task {@code taskPath}. */
    public int partOfShard(String taskPath, int shard) {
        String key = shardKey(taskPath, shard);
        return tests.getOrDefault(key, defaultPart(key));
    }

    /** The part running the non-test {@code check} work of {@code projectPath}. */
    public int homePart(String projectPath) {
        return projects.getOrDefault(projectPath, defaultPart(projectPath));
    }

    /** The split of {@code taskPath}, or {@code null} when it runs as a whole. */
    public Split split(String taskPath) {
        return splits.get(taskPath);
    }

    /** Whether test task {@code taskPath} (or any of its shards) runs in {@code part}. */
    public boolean runsInPart(String taskPath, int part) {
        return shardsInParts(taskPath, Set.of(part)).isEmpty() == false;
    }

    /**
     * The shards of {@code taskPath} that run in any of {@code requestedParts}. A task that is not split has a
     * single shard {@code 0}.
     */
    public Set<Integer> shardsInParts(String taskPath, Set<Integer> requestedParts) {
        Set<Integer> shards = new TreeSet<>();
        Split split = split(taskPath);
        if (split == null) {
            if (requestedParts.contains(partOfTest(taskPath))) {
                shards.add(0);
            }
        } else {
            for (int shard = 0; shard < split.count(); shard++) {
                if (requestedParts.contains(partOfShard(taskPath, shard))) {
                    shards.add(shard);
                }
            }
        }
        return shards;
    }

    int defaultPart(String key) {
        return 1 + CiShards.bucket(key, parts);
    }

    static String shardKey(String taskPath, int shard) {
        return taskPath + "#" + shard;
    }

    public static CiPartAssignment parse(String json) {
        try {
            return MAPPER.readValue(json, CiPartAssignment.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid CI part assignment", e);
        }
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this) + "\n";
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
