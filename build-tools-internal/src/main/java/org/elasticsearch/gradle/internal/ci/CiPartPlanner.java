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
import java.util.Arrays;
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
 * <p>The planner also decides which test tasks to split and into how many shards, from the durations of the items
 * a task can be split by (its test classes or YAML files, see {@link Durations#items()}): the fewest shards whose
 * longest one, fixed cost included, fits {@link Config#shardTargetSeconds()}. Shards are estimated with the same
 * {@link CiShards} buckets the build uses to pick a shard's items, so one long item is never spread thin.
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
     * @param pinnedSplits test tasks that always run as these shards, whatever their durations
     * @param antiAffinity groups of test task paths that must run in different parts
     * @param runFirstSeconds test tasks whose work in one part takes at least this long are listed in
     *                        {@link CiPartAssignment#runFirst()}; 0 lists none
     * @param heavyTestLimit  test tasks that start clusters a part runs at once ({@link CiHeavyTestThrottle}); 0 for no
     *                        limit beyond {@code workers}
     * @param shardOverheadSeconds the fixed cost of one execution of a split test task, for tasks without a measured
     *                             one in {@link Durations#overheads()}
     * @param shardTargetSeconds   the longest a shard of a split test task should take, fixed cost included; test tasks
     *                             with {@link Durations#items()} are split into the fewest shards that fit it. 0 splits
     *                             only the pinned tasks
     * @param maxShards            the most shards the planner splits a test task into
     * @param neverSplit           test tasks the planner must not split (for example one whose items are not all run
     *                             by CI)
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Config(
        @JsonProperty("parts") int parts,
        @JsonProperty("workers") int workers,
        @JsonProperty("pinnedSplits") Map<String, CiPartAssignment.Split> pinnedSplits,
        @JsonProperty("antiAffinity") List<List<String>> antiAffinity,
        @JsonProperty("runFirstSeconds") int runFirstSeconds,
        @JsonProperty("heavyTestLimit") int heavyTestLimit,
        @JsonProperty("shardOverheadSeconds") int shardOverheadSeconds,
        @JsonProperty("shardTargetSeconds") int shardTargetSeconds,
        @JsonProperty("maxShards") int maxShards,
        @JsonProperty("neverSplit") List<String> neverSplit
    ) {
        static final int DEFAULT_MAX_SHARDS = 8;

        public Config {
            if (parts < 1 || workers < 1) {
                throw new IllegalArgumentException("parts and workers must be positive");
            }
            if (runFirstSeconds < 0 || heavyTestLimit < 0 || shardOverheadSeconds < 0 || shardTargetSeconds < 0 || maxShards < 0) {
                throw new IllegalArgumentException(
                    "runFirstSeconds, heavyTestLimit, shardOverheadSeconds, shardTargetSeconds and maxShards must not be negative"
                );
            }
            pinnedSplits = pinnedSplits == null ? Map.of() : pinnedSplits;
            antiAffinity = antiAffinity == null ? List.of() : antiAffinity;
            maxShards = maxShards == 0 ? DEFAULT_MAX_SHARDS : maxShards;
            neverSplit = neverSplit == null ? List.of() : neverSplit;
        }

        public Config(
            int parts,
            int workers,
            Map<String, CiPartAssignment.Split> pinnedSplits,
            List<List<String>> antiAffinity,
            int runFirstSeconds,
            int heavyTestLimit,
            int shardOverheadSeconds
        ) {
            this(parts, workers, pinnedSplits, antiAffinity, runFirstSeconds, heavyTestLimit, shardOverheadSeconds, 0, 0, List.of());
        }

        public Config(int parts, int workers, Map<String, CiPartAssignment.Split> pinnedSplits, List<List<String>> antiAffinity) {
            this(parts, workers, pinnedSplits, antiAffinity, 0, 0, 0);
        }
    }

    /**
     * Median executed durations in seconds ({@code .ci/test-parts/durations.json}), exported from CI by
     * {@code .buildkite/scripts/ci-parts/export.ts}. Only measured values: shard estimates are derived here.
     *
     * @param tests     test task path to the duration of one execution of the whole task
     * @param projects  project path to the summed duration of its non-test tasks, without the ones its tests depend
     *                  on (compilation, jars), which every part running one of its test tasks runs anyway
     * @param overheads optional measured fixed cost of one execution of a test task, paid again by every shard
     * @param items     for test tasks that could be split, the duration of each item a shard would run: top-level test
     *                  classes, or the YAML files of a single YAML suite
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Durations(
        @JsonProperty("tests") Map<String, Double> tests,
        @JsonProperty("projects") Map<String, Double> projects,
        @JsonProperty("overheads") Map<String, Double> overheads,
        @JsonProperty("items") Map<String, Items> items
    ) {
        public Durations {
            tests = tests == null ? Map.of() : tests;
            projects = projects == null ? Map.of() : projects;
            overheads = overheads == null ? Map.of() : overheads;
            items = items == null ? Map.of() : items;
        }

        public Durations(Map<String, Double> tests, Map<String, Double> projects) {
            this(tests, projects, Map.of(), Map.of());
        }
    }

    /**
     * The durations of the items a test task's shards would run.
     *
     * @param by      what the items are, which is also how the task would be split
     * @param seconds item name (a top-level class name, or a YAML suite path {@code <api>/<file name>}) to duration
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Items(@JsonProperty("by") CiPartAssignment.SplitBy by, @JsonProperty("seconds") Map<String, Double> seconds) {
        public Items {
            if (by == null) {
                throw new IllegalArgumentException("items need a 'by' mode");
            }
            seconds = seconds == null ? Map.of() : seconds;
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
        Map<String, CiPartAssignment.Split> splits = splits(config, durations);
        Parts parts = new Parts(config);
        Map<String, Set<String>> antiAffinity = antiAffinityIndex(config.antiAffinity());
        Map<String, Integer> placedTasks = new HashMap<>();
        Map<String, Set<Integer>> shardParts = new HashMap<>();
        Map<String, Integer> tests = new TreeMap<>();
        // the part holding each project's heaviest test unit; its non-test work goes there too when that is free
        Map<String, Integer> heaviestTestPart = new HashMap<>();

        for (Unit unit : testUnits(splits, config, durations)) {
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
        return new CiPartAssignment(config.parts(), splits, tests, projects, runFirst(parts.chains, config.runFirstSeconds()));
    }

    /**
     * How every split test task is split: the pinned ones as configured, and each other task with item durations into
     * the fewest shards whose longest one, fixed cost included, fits {@link Config#shardTargetSeconds()}. When no count
     * up to {@link Config#maxShards()} fits (one item alone is too long), the count with the shortest longest shard wins,
     * the fewest shards on a tie. A task that fits as a whole is not split.
     */
    static Map<String, CiPartAssignment.Split> splits(Config config, Durations durations) {
        Map<String, CiPartAssignment.Split> splits = new TreeMap<>(config.pinnedSplits());
        if (config.shardTargetSeconds() == 0) {
            return splits;
        }
        Set<String> neverSplit = new HashSet<>(config.neverSplit());
        for (Map.Entry<String, Items> e : new TreeMap<>(durations.items()).entrySet()) {
            String taskPath = e.getKey();
            Double seconds = durations.tests().get(taskPath);
            if (seconds == null || splits.containsKey(taskPath) || neverSplit.contains(taskPath)) {
                continue;
            }
            int count = shardCount(taskPath, seconds, e.getValue(), config, durations);
            if (count > 1) {
                splits.put(taskPath, new CiPartAssignment.Split(count, e.getValue().by()));
            }
        }
        return splits;
    }

    private static int shardCount(String taskPath, double seconds, Items items, Config config, Durations durations) {
        double overhead = overhead(taskPath, seconds, config, durations);
        double work = work(seconds, overhead);
        int best = 1;
        long bestLongest = Long.MAX_VALUE;
        for (int count = 1; count <= config.maxShards(); count++) {
            double longest = overhead + Arrays.stream(shardWork(items, work, count)).max().orElse(0);
            if (longest <= config.shardTargetSeconds()) {
                return count;
            }
            if (Math.round(longest) < bestLongest) {
                best = count;
                bestLongest = Math.round(longest);
            }
        }
        return best;
    }

    /** The fixed cost of one execution of a test task: measured if there is a measurement, else the configured default. */
    private static double overhead(String taskPath, double seconds, Config config, Durations durations) {
        return Math.min(seconds, durations.overheads().getOrDefault(taskPath, (double) config.shardOverheadSeconds()));
    }

    /**
     * The work of a test task without its fixed cost. At least a fifth of the task, so that a fixed cost measured
     * higher than it really is cannot make the task look free to split.
     */
    private static double work(double seconds, double overhead) {
        return Math.max(seconds - overhead, 0.2 * seconds);
    }

    /**
     * The estimated work of each shard when {@code items} are split into {@code count} shards by the same buckets the
     * build uses: each shard's share of the task's work by its items' durations, and at least its longest item, since
     * one long class or file cannot be spread over shards.
     */
    static double[] shardWork(Items items, double work, int count) {
        double[] shards = new double[count];
        if (count == 1) {
            shards[0] = work;
            return shards;
        }
        double[] sum = new double[count];
        double[] longest = new double[count];
        double total = 0;
        for (Map.Entry<String, Double> item : new TreeMap<>(items.seconds()).entrySet()) {
            int shard = CiShards.bucket(item.getKey(), count);
            sum[shard] += item.getValue();
            longest[shard] = Math.max(longest[shard], item.getValue());
            total += item.getValue();
        }
        for (int shard = 0; shard < count; shard++) {
            shards[shard] = total > 0 ? Math.max(work * sum[shard] / total, longest[shard]) : work / count;
        }
        return shards;
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
        for (Unit unit : testUnits(assignment.splits(), config, durations)) {
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

    private static List<Unit> testUnits(Map<String, CiPartAssignment.Split> splits, Config config, Durations durations) {
        List<Unit> units = new ArrayList<>();
        for (Map.Entry<String, Double> e : durations.tests().entrySet()) {
            String taskPath = e.getKey();
            CiPartAssignment.Split split = splits.get(taskPath);
            if (split == null) {
                units.add(new Unit(taskPath, taskPath, -1, projectOf(taskPath), e.getValue()));
            } else {
                double overhead = overhead(taskPath, e.getValue(), config, durations);
                double work = work(e.getValue(), overhead);
                // estimates from the items' durations beat an even share, since one long class or file can dominate a
                // shard; either way every shard pays the fixed cost
                Items items = durations.items().get(taskPath);
                double[] shardWork = items != null && items.by() == split.by() ? shardWork(items, work, split.count()) : null;
                for (int shard = 0; shard < split.count(); shard++) {
                    String key = CiPartAssignment.shardKey(taskPath, shard);
                    double seconds = shardWork != null ? shardWork[shard] : work / split.count();
                    units.add(new Unit(key, taskPath, shard, projectOf(taskPath), seconds + overhead));
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
