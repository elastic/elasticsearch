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
    public void testSameProjectTasksAreSpreadBecauseTheyRunSerially() {
        // without the configuration cache two tasks of :server would run one after another in one part
        CiPartPlanner.Config config = new CiPartPlanner.Config(2, 8, Map.of(), List.of());
        CiPartPlanner.Durations durations = new CiPartPlanner.Durations(
            Map.of(":server:test", 600.0, ":server:internalClusterTest", 590.0, ":a:test", 100.0, ":b:test", 100.0),
            Map.of()
        );
        CiPartAssignment assignment = CiPartPlanner.plan(config, durations);
        assertThat(assignment.partOfTest(":server:test"), not(equalTo(assignment.partOfTest(":server:internalClusterTest"))));
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
            1,
            Map.of(":x:yamlRestTest", new CiPartAssignment.Split(3, CiPartAssignment.SplitBy.YAML_FILE)),
            List.of()
        );
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
    public void testProjectOf() {
        assertThat(CiPartPlanner.projectOf(":server:test"), equalTo(":server"));
        assertThat(CiPartPlanner.projectOf(":test"), equalTo(":"));
        assertThat(CiPartPlanner.projectOf(":qa:x:v9.1.0#bwcTest"), equalTo(":qa:x"));
    }
}
