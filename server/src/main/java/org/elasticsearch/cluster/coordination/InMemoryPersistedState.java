/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
