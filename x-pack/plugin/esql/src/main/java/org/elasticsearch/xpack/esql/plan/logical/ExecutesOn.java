/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * Mark nodes that execute only in a specific way, either on the coordinator or on a remote node.
 */
public interface ExecutesOn {
    enum ExecuteLocation {
        COORDINATOR,
        REMOTE,
        ANY; // Can be executed on either coordinator or remote nodes
    }

    ExecuteLocation executesOn();

    /**
     * Executes on the remote nodes only (note that may include coordinator, but not on the aggregation stage).
     */
    interface Remote extends ExecutesOn {
        @Override
        default ExecuteLocation executesOn() {
            return ExecuteLocation.REMOTE;
        }
    }

    /**
     * Executes on the coordinator only. Can not be run on remote nodes.
     */
    interface Coordinator extends ExecutesOn {
        @Override
        default ExecuteLocation executesOn() {
            return ExecuteLocation.COORDINATOR;
        }
    }
}
