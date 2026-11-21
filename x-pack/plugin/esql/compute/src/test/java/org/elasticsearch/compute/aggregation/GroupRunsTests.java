/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GroupRunsTests extends ComputeTestCase {
    public void testRunsCoverThePageInOrder() {
        GroupRuns runs = new GroupRuns(blockFactory());
        runs.appendRunEnd(3);
        runs.appendRunEnd(4);
        runs.appendRunEnd(9);
        runs.setGroupId(0, 7);
        runs.setGroupId(1, 2);
        runs.setGroupId(2, 7);

        assertThat(runs.runCount(), equalTo(3));
        assertThat(runs.isSingleRun(), equalTo(false));
        assertThat(runs.positionCount(), equalTo(9));
        assertThat(runs.start(0), equalTo(0));
        assertThat(runs.end(0), equalTo(3));
        assertThat(runs.length(0), equalTo(3));
        assertThat(runs.start(1), equalTo(3));
        assertThat(runs.length(1), equalTo(1));
        assertThat(runs.start(2), equalTo(4));
        assertThat(runs.length(2), equalTo(5));
        assertThat(runs.groupId(2), equalTo(7));
    }

    public void testToVectorExpandsEveryRun() {
        GroupRuns runs = new GroupRuns(blockFactory());
        runs.appendRunEnd(2);
        runs.appendRunEnd(3);
        runs.appendRunEnd(6);
        runs.setGroupId(0, 1);
        runs.setGroupId(1, 4);
        runs.setGroupId(2, 1);
        try (IntVector vector = runs.toVector()) {
            assertThat(vector.getPositionCount(), equalTo(6));
            int[] expected = { 1, 1, 4, 1, 1, 1 };
            for (int p = 0; p < expected.length; p++) {
                assertThat("position " + p, vector.getInt(p), equalTo(expected[p]));
            }
        }
    }

    public void testSingleRunIsAConstantVector() {
        GroupRuns runs = new GroupRuns(blockFactory());
        runs.appendRunEnd(100);
        runs.setGroupId(0, 5);
        assertThat(runs.isSingleRun(), equalTo(true));
        try (IntVector vector = runs.toVector()) {
            assertThat(vector.isConstant(), equalTo(true));
            assertThat(vector.getPositionCount(), equalTo(100));
            assertThat(vector.getInt(99), equalTo(5));
        }
    }

    public void testResetAndGrow() {
        GroupRuns runs = new GroupRuns(blockFactory());
        int count = between(100, 1000);
        for (int r = 0; r < count; r++) {
            runs.appendRunEnd(r + 1);
            runs.setGroupId(r, r * 2);
        }
        assertThat(runs.runCount(), equalTo(count));
        assertThat(runs.groupId(count - 1), equalTo((count - 1) * 2));
        runs.reset();
        assertThat(runs.runCount(), equalTo(0));
        assertThat(runs.positionCount(), equalTo(0));
    }
}
