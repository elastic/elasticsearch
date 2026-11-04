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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Generates a {@link CiPartAssignment} that balances historical test durations across CI parts.
 *
 * <p>Each unit (a test task, a shard of a split test task, or a project's non-test {@code check} work) is
 * placed longest first onto the part where it finishes earliest under a simple model of one CI job: a part
 * runs {@code workers} tasks at a time (the part jobs use the configuration cache, so tasks of one project run
 * in parallel too), but one test task is a single execution, however many of its shards the part runs. A part's
 * estimated finish time is therefore {@code max(total / workers, longest test task)}. A project's non-test work
 * is many short tasks (precommit and the like), so it only adds to the total. {@link Config#antiAffinity()}
 * keeps test tasks that must not share a part apart regardless of durations.
 *
 * <p>The result depends only on the inputs: units are visited in a fixed order and ties broken by part number.
 */
public final class CiPartPlanner {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Hand-maintained planner input ({@code .ci/test-parts/config.json}).
     *
     * @param parts        number of CI parts
     * @param workers      effective parallel test tasks per CI part (calibrated from CI timelines)
     * @param splits       test tasks that run as several shards
     * @param antiAffinity groups of test task paths that must run in different parts
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Config(
        @JsonProperty("parts") int parts,
        @JsonProperty("workers") int workers,
        @JsonProperty("splits") Map<String, CiPartAssignment.Split> splits,
        @JsonProperty("antiAffinity") List<List<String>> antiAffinity
    ) {
        public Config {
            if (parts < 1 || workers < 1) {
                throw new IllegalArgumentException("parts and workers must be positive");
            }
            splits = splits == null ? Map.of() : splits;
            antiAffinity = antiAffinity == null ? List.of() : antiAffinity;
        }
    }

    /**
     * Median executed durations in seconds ({@code .ci/test-parts/durations.json}).
     *
     * @param tests    test task path to duration
     * @param shards   optional estimate per shard of a split test task ({@code <task path>#<shard>}); shards without
     *                 one get an even share of the task's duration
     * @param projects project path to the summed duration of its non-test tasks, without the ones its tests depend on
     *                 (compilation, jars), which every part running one of its test tasks runs anyway
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Durations(
        @JsonProperty("tests") Map<String, Double> tests,
        @JsonProperty("shards") Map<String, Double> shards,
        @JsonProperty("projects") Map<String, Double> projects
    ) {
        public Durations {
            tests = tests == null ? Map.of() : tests;
            shards = shards == null ? Map.of() : shards;
            projects = projects == null ? Map.of() : projects;
        }
    }

    /**
     * A placeable piece of work; {@code shard} is -1 unless it is a shard of a split test task, and {@code taskPath}
     * is null for a project's non-test work.
     */
    private record Unit(String key, String taskPath, int shard, String project, double seconds) {}

    private CiPartPlanner() {}

    public static Config parseConfig(String json) {
        return read(json, Config.class);
    }

    public static Durations parseDurations(String json) {
        return read(json, Durations.class);
    }

    private static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid " + type.getSimpleName(), e);
        }
    }

    public static CiPartAssignment plan(Config config, Durations durations) {
        int parts = config.parts();
        double[] load = new double[parts];
        List<Map<String, Double>> chains = new ArrayList<>();
        for (int i = 0; i < parts; i++) {
            chains.add(new HashMap<>());
        }
        Map<String, Set<String>> antiAffinity = antiAffinityIndex(config.antiAffinity());
        Map<String, Integer> placedTasks = new HashMap<>();
        Map<String, Integer> tests = new TreeMap<>();
        // the part holding each project's heaviest test unit; its non-test work goes there too when that is free
        Map<String, Integer> heaviestTestPart = new HashMap<>();

        for (Unit unit : testUnits(config, durations)) {
            String taskPath = unit.taskPath();
            Set<Integer> forbidden = new HashSet<>();
            for (String other : antiAffinity.getOrDefault(taskPath, Set.of())) {
                Integer otherPart = placedTasks.get(other);
                if (otherPart != null) {
                    forbidden.add(otherPart);
                }
            }
            int part = bestPart(unit, load, chains, config.workers(), forbidden);
            place(unit, part, load, chains);
            tests.put(unit.key(), part + 1);
            placedTasks.putIfAbsent(taskPath, part);
            heaviestTestPart.putIfAbsent(unit.project(), part);
        }

        Map<String, Integer> projects = new TreeMap<>();
        for (Unit unit : sortedUnits(projectUnits(durations))) {
            int part = homePart(unit, heaviestTestPart.get(unit.project()), load, chains, config.workers());
            place(unit, part, load, chains);
            projects.put(unit.key(), part + 1);
        }
        return new CiPartAssignment(parts, config.splits(), tests, projects);
    }

    /**
     * Estimated duration in minutes of each part under the planner's model (index 0 is part 1). Useful to
     * review a generated assignment; it ignores fixed per-job overhead such as checkout and compilation.
     */
    public static double[] estimatedMinutes(CiPartAssignment assignment, Config config, Durations durations) {
        double[] load = new double[assignment.parts()];
        List<Map<String, Double>> chains = new ArrayList<>();
        for (int i = 0; i < assignment.parts(); i++) {
            chains.add(new HashMap<>());
        }
        for (Unit unit : testUnits(config, durations)) {
            int part = unit.shard() < 0 ? assignment.partOfTest(unit.taskPath()) : assignment.partOfShard(unit.taskPath(), unit.shard());
            place(unit, part - 1, load, chains);
        }
        for (Unit unit : projectUnits(durations)) {
            place(unit, assignment.homePart(unit.key()) - 1, load, chains);
        }
        double[] minutes = new double[assignment.parts()];
        for (int i = 0; i < minutes.length; i++) {
            minutes[i] = finish(load[i], chains.get(i), config.workers()) / 60;
        }
        return minutes;
    }

    private static List<Unit> testUnits(Config config, Durations durations) {
        List<Unit> units = new ArrayList<>();
        for (Map.Entry<String, Double> e : durations.tests().entrySet()) {
            String taskPath = e.getKey();
            CiPartAssignment.Split split = config.splits().get(taskPath);
            if (split == null) {
                units.add(new Unit(taskPath, taskPath, -1, projectOf(taskPath), e.getValue()));
            } else {
                for (int shard = 0; shard < split.count(); shard++) {
                    String key = CiPartAssignment.shardKey(taskPath, shard);
                    // a shard's own estimate (from per-class or per-file durations) beats an even share, since one
                    // long class or file can dominate a shard
                    double seconds = durations.shards().getOrDefault(key, e.getValue() / split.count());
                    units.add(new Unit(key, taskPath, shard, projectOf(taskPath), seconds));
                }
            }
        }
        return sortedUnits(units);
    }

    private static List<Unit> projectUnits(Durations durations) {
        List<Unit> units = new ArrayList<>();
        Set<String> projects = new TreeSet<>(durations.projects().keySet());
        durations.tests().keySet().forEach(t -> projects.add(projectOf(t)));
        for (String project : projects) {
            units.add(new Unit(project, null, -1, project, durations.projects().getOrDefault(project, 0.0)));
        }
        return units;
    }

    private static List<Unit> sortedUnits(List<Unit> units) {
        List<Unit> sorted = new ArrayList<>(units);
        sorted.sort(Comparator.comparingDouble(Unit::seconds).reversed().thenComparing(Unit::key));
        return sorted;
    }

    /**
     * The part for a project's non-test work: the part of its heaviest test unit, which keeps the project's work
     * together, unless that would make the slowest part slower than placing the work where it finishes earliest.
     */
    private static int homePart(Unit unit, Integer preferred, double[] load, List<Map<String, Double>> chains, int workers) {
        int best = bestPart(unit, load, chains, workers, Set.of());
        if (preferred == null) {
            return best;
        }
        double makespan = 0;
        for (int part = 0; part < load.length; part++) {
            makespan = Math.max(makespan, finish(load[part], chains.get(part), workers));
        }
        double bound = Math.max(makespan, finishWith(unit, best, load, chains, workers));
        return finishWith(unit, preferred, load, chains, workers) <= bound + 1e-9 ? preferred : best;
    }

    private static int bestPart(Unit unit, double[] load, List<Map<String, Double>> chains, int workers, Set<Integer> forbidden) {
        int best = -1;
        double bestFinish = Double.MAX_VALUE;
        for (int part = 0; part < load.length; part++) {
            if (forbidden.contains(part) && forbidden.size() < load.length) {
                continue;
            }
            double finish = finishWith(unit, part, load, chains, workers);
            if (finish < bestFinish - 1e-9 || (Math.abs(finish - bestFinish) <= 1e-9 && load[part] < load[best])) {
                best = part;
                bestFinish = finish;
            }
        }
        return best;
    }

    /** The finish time of {@code part} if {@code unit} were placed on it. */
    private static double finishWith(Unit unit, int part, double[] load, List<Map<String, Double>> chains, int workers) {
        double finish = Math.max((load[part] + unit.seconds()) / workers, finish(load[part], chains.get(part), workers));
        if (unit.taskPath() != null) {
            finish = Math.max(finish, chains.get(part).getOrDefault(unit.taskPath(), 0.0) + unit.seconds());
        }
        return finish;
    }

    private static double finish(double load, Map<String, Double> chains, int workers) {
        double longestChain = chains.values().stream().mapToDouble(Double::doubleValue).max().orElse(0);
        return Math.max(load / workers, longestChain);
    }

    private static void place(Unit unit, int part, double[] load, List<Map<String, Double>> chains) {
        load[part] += unit.seconds();
        // shards of one split test task in the same part run as one execution of that task
        if (unit.taskPath() != null) {
            chains.get(part).merge(unit.taskPath(), unit.seconds(), Double::sum);
        }
    }

    private static Map<String, Set<String>> antiAffinityIndex(List<List<String>> groups) {
        Map<String, Set<String>> index = new HashMap<>();
        for (List<String> group : groups) {
            for (String task : group) {
                for (String other : group) {
                    if (task.equals(other) == false) {
                        index.computeIfAbsent(task, k -> new TreeSet<>()).add(other);
                    }
                }
            }
        }
        return index;
    }

    /** The project path of a task path, e.g. {@code :server} for {@code :server:test} and {@code :} for {@code :test}. */
    static String projectOf(String taskPath) {
        int colon = taskPath.lastIndexOf(':');
        return colon <= 0 ? ":" : taskPath.substring(0, colon);
    }
}
