/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformNodeAssignmentsTests extends ESTestCase {

    public void testConstructorAndGetters() {
        Set<String> executorNodes = Set.of("executor-1", "executor-2");
        Set<String> assigned = Set.of("assigned-1", "assigned-2");
        Set<String> waitingForAssignment = Set.of("waiting-1", "waiting-2");
        Set<String> stopped = Set.of("stopped-1", "stopped-2");

        TransformNodeAssignments assignments = new TransformNodeAssignments(executorNodes, assigned, waitingForAssignment, stopped);

        assertThat(assignments.getExecutorNodes(), is(equalTo(executorNodes)));
        assertThat(assignments.getAssigned(), is(equalTo(assigned)));
        assertThat(assignments.getWaitingForAssignment(), is(equalTo(waitingForAssignment)));
        assertThat(assignments.getStopped(), is(equalTo(stopped)));
    }

    public void testToString() {
        Set<String> executorNodes = Set.of("executor-1");
        Set<String> assigned = Set.of("assigned-1");
        Set<String> waitingForAssignment = Set.of("waiting-1");
        Set<String> stopped = Set.of("stopped-1");

        TransformNodeAssignments assignments = new TransformNodeAssignments(executorNodes, assigned, waitingForAssignment, stopped);

        assertThat(
            assignments.toString(),
            is(
                equalTo(
                    "TransformNodeAssignments["
                        + "executorNodes=[executor-1],"
                        + "assigned=[assigned-1],"
                        + "waitingForAssignment=[waiting-1],"
                        + "stopped=[stopped-1]"
                        + "]"
                )
            )
        );
    }

    public void testToString_EmptyCollections() {
        Set<String> executorNodes = Set.of();
        Set<String> assigned = Set.of();
        Set<String> waitingForAssignment = Set.of();
        Set<String> stopped = Set.of();

        TransformNodeAssignments assignments = new TransformNodeAssignments(executorNodes, assigned, waitingForAssignment, stopped);

        assertThat(
            assignments.toString(),
            is(
                equalTo(
                    "TransformNodeAssignments[" + "executorNodes=[]," + "assigned=[]," + "waitingForAssignment=[]," + "stopped=[]" + "]"
                )
            )
        );
    }
}
