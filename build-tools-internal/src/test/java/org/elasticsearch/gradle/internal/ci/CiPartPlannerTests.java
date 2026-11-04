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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            Map.of(),
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
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(Map.of(":x:test", 1000.0), Map.of(), Map.of());
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.partOfShard(":x:test", 0), not(equalTo(assignment.partOfShard(":x:test", 1))));
    }

    @Test
    public void testProjectWorkLeavesItsHeaviestTestPartWhenThatWouldBeSlowest() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(2, 1, Map.of(), List.of());
        // :a's 40s of non-test work next to :a:test would make that part 140s; next to :b:test it is 90s
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":a:test", 100.0, ":b:test", 50.0),
            Map.of(),
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
            Map.of(),
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
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(tests, Map.of(), Map.of());
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
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(Map.of(":x:yamlRestTest", 900.0, ":y:test", 10.0), Map.of(), Map.of(":y", 5.0));
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
    public void testShardEstimatesOverrideEvenShares() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            2,
            1,
            Map.of(":x:javaRestTest", new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.CLASS)),
            List.of()
        );
        // shard 0 holds one long class; an even split would put :y:test (300s) next to it
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":x:javaRestTest", 1000.0, ":y:test", 300.0),
            Map.of(":x:javaRestTest#0", 800.0, ":x:javaRestTest#1", 200.0),
            Map.of()
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.partOfTest(":y:test"), equalTo(assignment.partOfShard(":x:javaRestTest", 1)));
    }

    @Test
    public void testRunFirstListsLongTestTasksLongestFirst() {
        CiPartPlanner.Config config = new CiPartPlanner.Config(
            2,
            4,
            Map.of(":c:test", new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.CLASS)),
            List.of(),
            300
        );
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":a:test", 400.0, ":b:test", 900.0, ":c:test", 1200.0, ":d:test", 100.0),
            Map.of(),
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
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(tests, Map.of(), Map.of());
        assertThat(CiPartPlanner.plan(config, durations).toJson(), equalTo(CiPartPlanner.plan(config, durations).toJson()));
    }

    @Test
    public void testProjectOf() {
        assertThat(CiPartPlanner.projectOf(":server:test"), equalTo(":server"));
        assertThat(CiPartPlanner.projectOf(":test"), equalTo(":"));
        assertThat(CiPartPlanner.projectOf(":qa:x:v9.1.0#bwcTest"), equalTo(":qa:x"));
    }
}
