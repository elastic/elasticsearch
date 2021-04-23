/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;

public class InMemoryPersistedState implements CoordinationState.PersistedState {

    private long currentTerm;
    private ClusterState acceptedState;

    public InMemoryPersistedState(long term, ClusterState acceptedState) {
        this.currentTerm = term;
        this.acceptedState = acceptedState;

        assert currentTerm >= 0;
        assert getLastAcceptedState().term() <= currentTerm :
            "last accepted term " + getLastAcceptedState().term() + " cannot be above current term " + currentTerm;
    }

    @Override
    public void setCurrentTerm(long currentTerm) {
        assert this.currentTerm <= currentTerm;
        this.currentTerm = currentTerm;
    }

    @Override
    public void setLastAcceptedState(ClusterState clusterState) {
        this.acceptedState = clusterState;
    }

    @Override
    public long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public ClusterState getLastAcceptedState() {
        return acceptedState;
    }
}
