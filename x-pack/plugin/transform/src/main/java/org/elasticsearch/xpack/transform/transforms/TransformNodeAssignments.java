/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import java.util.Collections;
import java.util.Set;

/**
 * Record of transform tasks and their current persistent task state.
 *
 * This class is aimed to be used by start/stop and stats action.
 */
public final class TransformNodeAssignments {

    // set of nodes where requested transforms are executed on
    private final Set<String> executorNodes;
    // set of transforms that are currently assigned to a node
    private final Set<String> assigned;
    // set of transforms that currently wait for node assignment
    private final Set<String> waitingForAssignment;
    // set of transforms that have neither a task nor wait for assignment, so considered stopped
    private final Set<String> stopped;

    TransformNodeAssignments(
        final Set<String> executorNodes,
        final Set<String> assigned,
        final Set<String> waitingForAssignment,
        final Set<String> stopped
    ) {
        this.executorNodes = Collections.unmodifiableSet(executorNodes);
        this.assigned = Collections.unmodifiableSet(assigned);
        this.waitingForAssignment = Collections.unmodifiableSet(waitingForAssignment);
        this.stopped = Collections.unmodifiableSet(stopped);
    }

    /*
     * Get nodes where (requested) transforms are executed.
     */
    public Set<String> getExecutorNodes() {
        return executorNodes;
    }

    /*
     * Get transforms which have tasks currently assigned to a node
     */
    public Set<String> getAssigned() {
        return assigned;
    }

    /*
     * Get transforms which are currently waiting to be assigned to a node
     */
    public Set<String> getWaitingForAssignment() {
        return waitingForAssignment;
    }

    /*
     * Get transforms which have no tasks, which means they are stopped
     */
    public Set<String> getStopped() {
        return stopped;
    }
}
