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

import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class CiPartAssignmentTests {

    private final CiPartAssignment assignment = new CiPartAssignment(
        4,
        Map.of(":x:yamlRestTest", new CiPartAssignment.Split(3, CiPartAssignment.SplitBy.YAML_FILE)),
        Map.of(":server:test", 2, ":x:yamlRestTest#0", 1, ":x:yamlRestTest#1", 4, ":x:yamlRestTest#2", 1),
        Map.of(":server", 3)
    );

    @Test
    public void testListedEntriesWin() {
        assertThat(assignment.partOfTest(":server:test"), equalTo(2));
        assertThat(assignment.homePart(":server"), equalTo(3));
        assertThat(assignment.partOfShard(":x:yamlRestTest", 1), equalTo(4));
    }

    @Test
    public void testUnknownEntriesFallBackToTheHash() {
        assertThat(assignment.partOfTest(":new:test"), equalTo(1 + CiShards.bucket(":new:test", 4)));
        assertThat(assignment.homePart(":new"), equalTo(1 + CiShards.bucket(":new", 4)));
        assertThat(assignment.partOfShard(":new:test", 0), equalTo(1 + CiShards.bucket(":new:test#0", 4)));
    }

    @Test
    public void testShardsInParts() {
        assertThat(assignment.shardsInParts(":x:yamlRestTest", Set.of(1)), contains(0, 2));
        assertThat(assignment.shardsInParts(":x:yamlRestTest", Set.of(1, 4)), contains(0, 1, 2));
        assertThat(assignment.shardsInParts(":x:yamlRestTest", Set.of(2)), empty());
        assertThat(assignment.shardsInParts(":server:test", Set.of(2)), contains(0));
        assertThat(assignment.runsInPart(":server:test", 1), equalTo(false));
    }

    /** Every test task and every shard runs in exactly one part. */
    @Test
    public void testEveryShardRunsInExactlyOnePart() {
        for (String task : new String[] { ":server:test", ":x:yamlRestTest", ":new:test" }) {
            int shards = assignment.split(task) == null ? 1 : assignment.split(task).count();
            int total = 0;
            for (int part = 1; part <= assignment.parts(); part++) {
                total += assignment.shardsInParts(task, Set.of(part)).size();
            }
            assertThat(task, total, equalTo(shards));
        }
    }

    @Test
    public void testRoundTrip() {
        CiPartAssignment parsed = CiPartAssignment.parse(assignment.toJson());
        assertThat(parsed, equalTo(assignment));
        assertThat(assignment.toJson().contains("\"by\" : \"yamlFile\""), equalTo(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsPartsOutOfRange() {
        new CiPartAssignment(2, Map.of(), Map.of(":a:test", 3), Map.of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsTrivialSplits() {
        new CiPartAssignment.Split(1, CiPartAssignment.SplitBy.CLASS);
    }
}
