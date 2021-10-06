/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformNodeAssignmentsTests extends ESTestCase {

    public void testConstructorAndGetters() {
        Set<String> executorNodes = new HashSet<>(Arrays.asList("executor-1", "executor-2"));
        Set<String> assigned = new HashSet<>(Arrays.asList("assigned-1", "assigned-2"));
        Set<String> waitingForAssignment = new HashSet<>(Arrays.asList("waiting-1", "waitingv-2"));
        Set<String> stopped = new HashSet<>(Arrays.asList("stopped-1", "stopped-2"));
        TransformNodeAssignments assignments = new TransformNodeAssignments(executorNodes, assigned, waitingForAssignment, stopped);

        assertThat(assignments.getExecutorNodes(), is(equalTo(executorNodes)));
        assertThat(assignments.getAssigned(), is(equalTo(assigned)));
        assertThat(assignments.getWaitingForAssignment(), is(equalTo(waitingForAssignment)));
        assertThat(assignments.getStopped(), is(equalTo(stopped)));
    }
}
