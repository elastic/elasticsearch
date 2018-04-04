/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;

public class TerminalPolicyStep extends ClusterStateActionStep {
    public static final StepKey KEY = new StepKey("completed", "completed", "completed");
    public static final TerminalPolicyStep INSTANCE = new TerminalPolicyStep(KEY, null);

    private TerminalPolicyStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        return clusterState;
    }
}
