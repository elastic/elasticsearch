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
 * in parallel too), at most {@code heavyTestLimit} of them test tasks that start clusters (see
 * {@link CiHeavyTestThrottle}), and one test task is a single execution, however many of its shards the part runs.
 * A part's estimated finish time is therefore {@code max(total / workers, cluster tests / heavyTestLimit, longest
 * test task)}. Every shard of a split test task pays the task's fixed cost (a test JVM, cluster start-up, suite
 * setup) again, so splitting further is not free. A project's non-test work is many short tasks (precommit and the
 * like), so it only adds to the total. {@link Config#antiAffinity()}
 * keeps test tasks that must not share a part apart regardless of durations, and the shards of a split test
 * task go to different parts while there are parts left, since a task is split so that its shards run in parallel.
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
     * @param runFirstSeconds test tasks whose work in one part takes at least this long are listed in
     *                        {@link CiPartAssignment#runFirst()}; 0 lists none
     * @param heavyTestLimit  test tasks that start clusters a part runs at once ({@link CiHeavyTestThrottle}); 0 for no
     *                        limit beyond {@code workers}
     * @param shardOverheadSeconds the fixed cost of one execution of a split test task, for tasks without a measured
     *                             one in {@link Durations#overheads()}
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Config(
        @JsonProperty("parts") int parts,
        @JsonProperty("workers") int workers,
        @JsonProperty("splits") Map<String, CiPartAssignment.Split> splits,
        @JsonProperty("antiAffinity") List<List<String>> antiAffinity,
        @JsonProperty("runFirstSeconds") int runFirstSeconds,
        @JsonProperty("heavyTestLimit") int heavyTestLimit,
        @JsonProperty("shardOverheadSeconds") int shardOverheadSeconds
    ) {
        public Config {
            if (parts < 1 || workers < 1) {
                throw new IllegalArgumentException("parts and workers must be positive");
            }
            if (runFirstSeconds < 0 || heavyTestLimit < 0 || shardOverheadSeconds < 0) {
                throw new IllegalArgumentException("runFirstSeconds, heavyTestLimit and shardOverheadSeconds must not be negative");
            }
            splits = splits == null ? Map.of() : splits;
            antiAffinity = antiAffinity == null ? List.of() : antiAffinity;
        }

        public Config(int parts, int workers, Map<String, CiPartAssignment.Split> splits, List<List<String>> antiAffinity) {
            this(parts, workers, splits, antiAffinity, 0, 0, 0);
        }
    }

    /**
     * Median executed durations in seconds ({@code .ci/test-parts/durations.json}).
     *
     * @param tests     test task path to the duration of one execution of the whole task
     * @param shards    optional estimate per shard of a split test task ({@code <task path>#<shard>}) of its work,
     *                  without the task's fixed cost; shards without one get an even share of the task's work
     * @param projects  project path to the summed duration of its non-test tasks, without the ones its tests depend
     *                  on (compilation, jars), which every part running one of its test tasks runs anyway
     * @param overheads optional measured fixed cost of one execution of a test task, paid again by every shard
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Durations(
        @JsonProperty("tests") Map<String, Double> tests,
        @JsonProperty("shards") Map<String, Double> shards,
        @JsonProperty("projects") Map<String, Double> projects,
        @JsonProperty("overheads") Map<String, Double> overheads
    ) {
        public Durations {
            tests = tests == null ? Map.of() : tests;
            shards = shards == null ? Map.of() : shards;
            projects = projects == null ? Map.of() : projects;
            overheads = overheads == null ? Map.of() : overheads;
        }

        public Durations(Map<String, Double> tests, Map<String, Double> shards, Map<String, Double> projects) {
            this(tests, shards, projects, Map.of());
        }
    }

    /**
     * A placeable piece of work; {@code shard} is -1 unless it is a shard of a split test task, and {@code taskPath}
     * is null for a project's non-test work.
     */
    private record Unit(String key, String taskPath, int shard, String project, double seconds) {
        boolean heavy() {
            return taskPath != null && CiHeavyTestThrottle.isHeavy(taskPath.substring(taskPath.lastIndexOf(':') + 1));
        }
    }

    /** The model's state of every part while units are placed. */
    private static final class Parts {
        private final double[] load;
        private final double[] heavyLoad;
        private final List<Map<String, Double>> chains = new ArrayList<>();
        private final int workers;
        private final int heavyTestLimit;

        Parts(Config config) {
            load = new double[config.parts()];
            heavyLoad = new double[config.parts()];
            for (int i = 0; i < config.parts(); i++) {
                chains.add(new HashMap<>());
            }
            workers = config.workers();
            heavyTestLimit = config.heavyTestLimit();
        }

        int count() {
            return load.length;
        }

        double finish(int part) {
            return finish(load[part], heavyLoad[part], chains.get(part).values().stream().mapToDouble(Double::doubleValue).max().orElse(0));
        }

        /** The finish time of {@code part} if {@code unit} were placed on it. */
        double finishWith(Unit unit, int part) {
            double chain = chains.get(part).values().stream().mapToDouble(Double::doubleValue).max().orElse(0);
            if (unit.taskPath() != null) {
                chain = Math.max(chain, chains.get(part).getOrDefault(unit.taskPath(), 0.0) + unit.seconds());
            }
            return finish(load[part] + unit.seconds(), heavyLoad[part] + (unit.heavy() ? unit.seconds() : 0), chain);
        }

        private double finish(double partLoad, double partHeavyLoad, double longestChain) {
            double finish = Math.max(partLoad / workers, longestChain);
            return heavyTestLimit > 0 ? Math.max(finish, partHeavyLoad / heavyTestLimit) : finish;
        }

        void place(Unit unit, int part) {
            load[part] += unit.seconds();
            if (unit.heavy()) {
                heavyLoad[part] += unit.seconds();
            }
            // shards of one split test task in the same part run as one execution of that task
            if (unit.taskPath() != null) {
                chains.get(part).merge(unit.taskPath(), unit.seconds(), Double::sum);
            }
        }
    }

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
        Parts parts = new Parts(config);
        Map<String, Set<String>> antiAffinity = antiAffinityIndex(config.antiAffinity());
        Map<String, Integer> placedTasks = new HashMap<>();
        Map<String, Set<Integer>> shardParts = new HashMap<>();
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
            if (unit.shard() >= 0) {
                forbidden.addAll(shardParts.getOrDefault(taskPath, Set.of()));
            }
            int part = bestPart(unit, parts, forbidden);
            parts.place(unit, part);
            tests.put(unit.key(), part + 1);
            placedTasks.putIfAbsent(taskPath, part);
            if (unit.shard() >= 0) {
                shardParts.computeIfAbsent(taskPath, k -> new HashSet<>()).add(part);
            }
            heaviestTestPart.putIfAbsent(unit.project(), part);
        }

        Map<String, Integer> projects = new TreeMap<>();
        for (Unit unit : sortedUnits(projectUnits(durations))) {
            int part = homePart(unit, heaviestTestPart.get(unit.project()), parts);
            parts.place(unit, part);
            projects.put(unit.key(), part + 1);
        }
        return new CiPartAssignment(config.parts(), config.splits(), tests, projects, runFirst(parts.chains, config.runFirstSeconds()));
    }

    /** Test tasks whose work in their part takes at least {@code minSeconds}, longest first. */
    private static List<String> runFirst(List<Map<String, Double>> chains, int minSeconds) {
        if (minSeconds == 0) {
            return List.of();
        }
        Map<String, Double> longest = new TreeMap<>();
        for (Map<String, Double> partChains : chains) {
            partChains.forEach((taskPath, seconds) -> {
                if (seconds >= minSeconds) {
                    longest.merge(taskPath, seconds, Math::max);
                }
            });
        }
        List<String> sorted = new ArrayList<>(longest.keySet());
        sorted.sort(Comparator.comparing((String taskPath) -> longest.get(taskPath)).reversed().thenComparing(Comparator.naturalOrder()));
        return sorted;
    }

    /**
     * Estimated duration in minutes of each part under the planner's model (index 0 is part 1). Useful to
     * review a generated assignment; it ignores fixed per-job overhead such as checkout and compilation.
     */
    public static double[] estimatedMinutes(CiPartAssignment assignment, Config config, Durations durations) {
        Parts parts = new Parts(config);
        for (Unit unit : testUnits(config, durations)) {
            int part = unit.shard() < 0 ? assignment.partOfTest(unit.taskPath()) : assignment.partOfShard(unit.taskPath(), unit.shard());
            parts.place(unit, part - 1);
        }
        for (Unit unit : projectUnits(durations)) {
            parts.place(unit, assignment.homePart(unit.key()) - 1);
        }
        double[] minutes = new double[parts.count()];
        for (int i = 0; i < minutes.length; i++) {
            minutes[i] = parts.finish(i) / 60;
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
                double overhead = Math.min(
                    e.getValue(),
                    durations.overheads().getOrDefault(taskPath, (double) config.shardOverheadSeconds())
                );
                for (int shard = 0; shard < split.count(); shard++) {
                    String key = CiPartAssignment.shardKey(taskPath, shard);
                    // a shard's own estimate (from per-class or per-file durations) beats an even share, since one
                    // long class or file can dominate a shard; either way every shard pays the fixed cost
                    double work = durations.shards().getOrDefault(key, (e.getValue() - overhead) / split.count());
                    units.add(new Unit(key, taskPath, shard, projectOf(taskPath), work + overhead));
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
    private static int homePart(Unit unit, Integer preferred, Parts parts) {
        int best = bestPart(unit, parts, Set.of());
        if (preferred == null) {
            return best;
        }
        double makespan = 0;
        for (int part = 0; part < parts.count(); part++) {
            makespan = Math.max(makespan, parts.finish(part));
        }
        double bound = Math.max(makespan, parts.finishWith(unit, best));
        return parts.finishWith(unit, preferred) <= bound + 1e-9 ? preferred : best;
    }

    private static int bestPart(Unit unit, Parts parts, Set<Integer> forbidden) {
        int best = -1;
        double bestFinish = Double.MAX_VALUE;
        for (int part = 0; part < parts.count(); part++) {
            if (forbidden.contains(part) && forbidden.size() < parts.count()) {
                continue;
            }
            double finish = parts.finishWith(unit, part);
            if (finish < bestFinish - 1e-9 || (Math.abs(finish - bestFinish) <= 1e-9 && parts.load[part] < parts.load[best])) {
                best = part;
                bestFinish = finish;
            }
        }
        return best;
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
