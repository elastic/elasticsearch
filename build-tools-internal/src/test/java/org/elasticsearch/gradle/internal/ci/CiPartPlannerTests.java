/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class CiPartPlannerTests {

    @Test
    public void testTasksOfOneProjectRunInParallel() {
        // with the configuration cache :p:c runs alongside :p:a, so it fits next to it rather than next to the longer :q:b
        CiPartPlanner.Config config = new CiPartPlanner.Config(2, 8, Map.of(), List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":p:a", 1000.0, ":q:b", 1100.0, ":p:c", 500.0),
            Map.of()
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.partOfTest(":p:c"), equalTo(assignment.partOfTest(":p:a")));
    }

    @Test
    public void testShardsOfOneTaskInAPartRunAsOneExecution() {
        // plenty of workers, but two shards in one part are one execution of the task, so they take twice as long
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            2,
            8,
            Map.of(":x:test", new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.CLASS)),
            List.of()
        );
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(Map.of(":x:test", 1000.0), Map.of());
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.partOfShard(":x:test", 0), not(equalTo(assignment.partOfShard(":x:test", 1))));
    }

    @Test
    public void testProjectWorkLeavesItsHeaviestTestPartWhenThatWouldBeSlowest() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(2, 1, Map.of(), List.of());
        // :a's 40s of non-test work next to :a:test would make that part 140s; next to :b:test it is 90s
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":a:test", 100.0, ":b:test", 50.0),
            Map.of(":a", 40.0, ":b", 5.0)
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.homePart(":a"), equalTo(assignment.partOfTest(":b:test")));
        assertThat(assignment.homePart(":b"), equalTo(assignment.partOfTest(":b:test")));
        double[] minutes = CiPartPlanner.estimatedMinutes(assignment, config, durations);
        assertThat(Math.max(minutes[0], minutes[1]), equalTo(100.0 / 60));
    }

    @Test
    public void testAntiAffinityIsHonouredEvenWhenDurationsWouldNotSeparate() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(2, 100, Map.of(), List.of(List.of(":p:a", ":q:b")));
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":p:a", 10.0, ":q:b", 10.0, ":r:c", 1000.0),
            Map.of()
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.partOfTest(":p:a"), not(equalTo(assignment.partOfTest(":q:b"))));
    }

    @Test
    public void testBalancesLoad() {
        Map<String, Double> tests = new HashMap<>();
        for (int i = 0; i < 200; i++) {
            tests.put(":p" + i + ":test", 60.0 + (i * 7) % 300);
        }
        CiPartPlanner.Config config = new CiPartPlanner.Config(6, 4, Map.of(), List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(tests, Map.of());
        double[] minutes = CiPartPlanner.estimatedMinutes(CiPartPlanner.plan(config, durations), config, durations);
        double max = 0, min = Double.MAX_VALUE;
        for (double m : minutes) {
            max = Math.max(max, m);
            min = Math.min(min, m);
        }
        assertThat(max - min, lessThanOrEqualTo(5.0 / 4));
    }

    @Test
    public void testSplitTasksArePlacedPerShardAndProjectsFollowTheirHeaviestTest() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            3,
            8,
            Map.of(":x:yamlRestTest", new CiPartAssignment.Split(3, CiPartAssignment.SplitBy.YAML_FILE)),
            List.of()
        );
        // with 8 workers :y's non-test work next to :y:test costs nothing, so it stays with the project's tests
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(Map.of(":x:yamlRestTest", 900.0, ":y:test", 10.0), Map.of(":y", 5.0));
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.tests().keySet(), equalTo(java.util.Set.of(":x:yamlRestTest#0", ":x:yamlRestTest#1", ":x:yamlRestTest#2", ":y:test")));
        assertThat(
            java.util.Set.of(assignment.partOfShard(":x:yamlRestTest", 0), assignment.partOfShard(":x:yamlRestTest", 1), assignment.partOfShard(":x:yamlRestTest", 2)),
            equalTo(java.util.Set.of(1, 2, 3))
        );
        assertThat(assignment.homePart(":y"), equalTo(assignment.partOfTest(":y:test")));
        assertThat(assignment.homePart(":x"), equalTo(assignment.partOfShard(":x:yamlRestTest", 0)));
        assertThat(assignment.split(":x:yamlRestTest").count(), equalTo(3));
    }

    @Test
    public void testItemDurationsOverrideEvenShares() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            2,
            1,
            Map.of(":x:javaRestTest", new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.CLASS)),
            List.of()
        );
        // shard 0 holds one long class; an even split would put :y:test (300s) next to it
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":x:javaRestTest", 1000.0, ":y:test", 300.0),
            Map.of(),
            Map.of(),
            Map.of(":x:javaRestTest", classes(2, 800.0, 200.0))
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.partOfTest(":y:test"), equalTo(assignment.partOfShard(":x:javaRestTest", 1)));
    }

    @Test
    public void testHeavyTestLimitSpreadsClusterTestsAcrossParts() {
        // plenty of workers, so only the one-cluster-test-at-a-time limit makes the cluster tests compete
        CiPartPlanner.Config config = new CiPartPlanner.Config(2, 8, Map.of(), List.of(), 0, 1, 0);
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(
                ":a:javaRestTest",
                300.0,
                ":b:javaRestTest",
                300.0,
                ":c:yamlRestTest",
                300.0,
                ":d:yamlRestTest",
                300.0,
                ":a:test",
                350.0,
                ":b:test",
                350.0
            ),
            Map.of()
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        Map<Integer, Integer> clusterTestsPerPart = new HashMap<>();
        for (String task : List.of(":a:javaRestTest", ":b:javaRestTest", ":c:yamlRestTest", ":d:yamlRestTest")) {
            clusterTestsPerPart.merge(assignment.partOfTest(task), 1, Integer::sum);
        }
        assertThat(clusterTestsPerPart, equalTo(Map.of(1, 2, 2, 2)));
        // two cluster tests one after another in each part
        for (double minutes : CiPartPlanner.estimatedMinutes(assignment, config, durations)) {
            assertThat(minutes, equalTo(10.0));
        }
    }

    @Test
    public void testEveryShardPaysTheTaskOverhead() {
        // 600 s of work plus 60 s of fixed cost per execution: two shards are 360 s each, not 330
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            2,
            8,
            Map.of(":x:javaRestTest", new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.CLASS)),
            List.of(),
            0,
            0,
            60
        );
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(Map.of(":x:javaRestTest", 660.0), Map.of());
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        for (double minutes : CiPartPlanner.estimatedMinutes(assignment, config, durations)) {
            assertThat(minutes, equalTo(6.0));
        }
        // a measured overhead for the task wins over the default, and item durations are work only
        CiPartPlanner.Durations measured = new CiPartPlanner.Durations(
            Map.of(":x:javaRestTest", 660.0),
            Map.of(),
            Map.of(":x:javaRestTest", 120.0),
            Map.of(":x:javaRestTest", classes(2, 420.0, 120.0))
        );
        double[] minutes = CiPartPlanner.estimatedMinutes(CiPartPlanner.plan(config, measured), config, measured);
        Arrays.sort(minutes);
        assertThat(minutes[0], equalTo(4.0));
        assertThat(minutes[1], equalTo(9.0));
    }

    @Test
    public void testShardsOfOneTaskGoToDifferentPartsWhileThereAreParts() {
        // the two short shards would fit next to each other behind the long task's part, but they are split to run in parallel
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            3,
            8,
            Map.of(":heap:test", new CiPartAssignment.Split(4, CiPartAssignment.SplitBy.CLASS)),
            List.of()
        );
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":big:test", 1000.0, ":heap:test", 800.0),
            Map.of()
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        Set<Integer> parts = new HashSet<>();
        for (int shard = 0; shard < 3; shard++) {
            parts.add(assignment.partOfShard(":heap:test", shard));
        }
        // the first three shards take a part each; the fourth has to share
        assertThat(parts.size(), equalTo(3));
    }

    @Test
    public void testRunFirstListsLongTestTasksLongestFirst() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            2,
            4,
            Map.of(":c:test", new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.CLASS)),
            List.of(),
            300,
            0,
            0
        );
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":a:test", 400.0, ":b:test", 900.0, ":c:test", 1200.0, ":d:test", 100.0),
            Map.of(":a", 2000.0)
        );
        // :c:test's two shards land in different parts, 600 s each; :d:test is short and project work never runs first
        assertThat(CiPartPlanner.plan(config, durations).runFirst(), equalTo(List.of(":b:test", ":c:test", ":a:test")));
        assertThat(CiPartPlanner.plan(new CiPartPlanner.Config(2, 4, Map.of(), List.of()), durations).runFirst(), equalTo(List.of()));
    }

    @Test
    public void testDeterministic() {
        Map<String, Double> tests = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            tests.put(":p" + (i % 7) + ":t" + i, 100.0);
        }
        CiPartPlanner.Config config = new CiPartPlanner.Config(5, 3, Map.of(), List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(tests, Map.of());
        assertThat(CiPartPlanner.plan(config, durations).toJson(), equalTo(CiPartPlanner.plan(config, durations).toJson()));
    }

    @Test
    public void testSplitsATaskIntoTheFewestShardsThatFitTheTarget() {
        // 1200 s of work in four 300 s classes plus 60 s of fixed cost: two shards of 600 + 60 fit a 700 s target
        CiPartPlanner.Config config = config(700, 8, List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":x:javaRestTest", 1260.0, ":y:test", 100.0),
            Map.of(),
            Map.of(":x:javaRestTest", 60.0),
            Map.of(":x:javaRestTest", classes(2, 300.0, 300.0, 300.0, 300.0))
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.split(":x:javaRestTest"), equalTo(new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.CLASS)));
        assertThat(assignment.split(":y:test"), equalTo(null));
        assertThat(assignment.partOfShard(":x:javaRestTest", 0), not(equalTo(assignment.partOfShard(":x:javaRestTest", 1))));
    }

    @Test
    public void testDoesNotSplitATaskThatFitsTheTarget() {
        CiPartPlanner.Config config = config(700, 8, List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":x:javaRestTest", 650.0),
            Map.of(),
            Map.of(),
            Map.of(":x:javaRestTest", classes(2, 300.0, 300.0))
        );
        assertThat(CiPartPlanner.splits(config, durations), equalTo(Map.of()));
    }

    @Test
    public void testOneLongItemBoundsTheSplit() {
        // one 900 s class cannot be spread: more shards never get below it, so the fewest shards with the shortest longest
        // shard win (2: 900 + 0 and the rest), not the maximum
        CiPartPlanner.Config config = config(600, 8, List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":x:javaRestTest", 1200.0),
            Map.of(),
            Map.of(":x:javaRestTest", 0.0),
            Map.of(":x:javaRestTest", classes(2, 900.0, 300.0))
        );
        assertThat(CiPartPlanner.splits(config, durations).get(":x:javaRestTest").count(), equalTo(2));
        assertThat(CiPartPlanner.shardWork(classes(2, 900.0, 300.0), 1200.0, 2), equalTo(new double[] { 900.0, 300.0 }));
    }

    @Test
    public void testNeverSplitsIntoShardsWithoutItems() {
        // :qa:mixed-cluster's BWC yamlRestTest: one long YAML suite class and three short ones. Where the long class is
        // alone, more shards look no worse, but a shard without any class would start its clusters to run nothing.
        Map<String, Double> seconds = Map.of(
            "org.elasticsearch.backwards.HotThreadsIT",
            83.5,
            "org.elasticsearch.backwards.IndexingIT",
            21.7,
            "org.elasticsearch.backwards.MixedClusterClientYamlTestSuiteIT",
            986.9,
            "org.elasticsearch.backwards.RareTermsIT",
            8.0
        );
        CiPartPlanner.Items items = new CiPartPlanner.Items(CiPartAssignment.SplitBy.CLASS, seconds);
        String task = ":qa:mixed-cluster:v{V}#yamlRestTest";
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(task, 1111.0),
            Map.of(),
            Map.of(task, 17.0),
            Map.of(task, items)
        );
        CiPartAssignment.Split split = CiPartPlanner.splits(config(600, 8, List.of()), durations).get(task);
        int count = split == null ? 1 : split.count();
        Set<Integer> used = new HashSet<>();
        seconds.keySet().forEach(name -> used.add(CiShards.bucket(name, count)));
        assertThat("shards of " + count, used.size(), equalTo(count));
    }

    @Test
    public void testYamlFileItemsSplitByYamlFile() {
        CiPartPlanner.Config config = config(400, 8, List.of());
        Map<String, Double> files = new HashMap<>();
        for (int i = 0; i < 40; i++) {
            files.put("api" + i + "/10_basic", 30.0);
        }
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":x:yamlRestTest", 1200.0),
            Map.of(),
            Map.of(":x:yamlRestTest", 0.0),
            Map.of(":x:yamlRestTest", new CiPartPlanner.Items(CiPartAssignment.SplitBy.YAML_FILE, files))
        );
        CiPartAssignment.Split split = CiPartPlanner.splits(config, durations).get(":x:yamlRestTest");
        assertThat(split.by(), equalTo(CiPartAssignment.SplitBy.YAML_FILE));
        assertThat(split.count() >= 3, equalTo(true));
        assertThat(split.count() <= config.maxShards(), equalTo(true));
    }

    @Test
    public void testPinnedAndNeverSplitTasksAreNotPlanned() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            2,
            8,
            Map.of(":p:test", new CiPartAssignment.Split(3, CiPartAssignment.SplitBy.CLASS)),
            List.of(),
            0,
            0,
            0,
            100,
            8,
            List.of(":n:test")
        );
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":p:test", 1000.0, ":n:test", 1000.0),
            Map.of(),
            Map.of(),
            Map.of(":p:test", classes(2, 500.0, 500.0), ":n:test", classes(2, 500.0, 500.0))
        );
        assertThat(
            CiPartPlanner.splits(config, durations),
            equalTo(Map.of(":p:test", new CiPartAssignment.Split(3, CiPartAssignment.SplitBy.CLASS)))
        );
    }

    @Test
    public void testNoTargetSplitsOnlyPinnedTasks() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(2, 8, Map.of(), List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":x:test", 5000.0),
            Map.of(),
            Map.of(),
            Map.of(":x:test", classes(2, 2500.0, 2500.0))
        );
        assertThat(CiPartPlanner.plan(config, durations).splits(), equalTo(Map.of()));
    }

    @Test
    public void testProjectOf() {
        assertThat(CiPartPlanner.projectOf(":server:test"), equalTo(":server"));
        assertThat(CiPartPlanner.projectOf(":test"), equalTo(":"));
        assertThat(CiPartPlanner.projectOf(":qa:x:v9.1.0#bwcTest"), equalTo(":qa:x"));
    }

    private static CiPartPlanner.Config config(int shardTargetSeconds, int maxShards, List<String> neverSplit) {
        return new CiPartPlanner.Config(2, 8, Map.of(), List.of(), 0, 0, 0, shardTargetSeconds, maxShards, neverSplit);
    }

    /**
     * Class items with the given durations, named so that with {@code count} shards the i-th item lands in shard
     * {@code i % count}: shard membership comes from a hash of the name, so tests pick names by their bucket.
     */
    private static CiPartPlanner.Items classes(int count, double... seconds) {
        Map<String, Double> items = new HashMap<>();
        int candidate = 0;
        for (int i = 0; i < seconds.length; i++) {
            String name;
            do {
                name = "org.example.Test" + candidate++;
            } while (CiShards.bucket(name, count) != i % count);
            items.put(name, seconds[i]);
        }
        return new CiPartPlanner.Items(CiPartAssignment.SplitBy.CLASS, items);
    }
}
