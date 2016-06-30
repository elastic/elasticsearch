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
package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.AliasAction;

/**
 * Cluster state update request that allows to add or remove aliases
 */
public class IndicesAliasesClusterStateUpdateRequest extends ClusterStateUpdateRequest<IndicesAliasesClusterStateUpdateRequest> {

    AliasAction[] actions;

    public IndicesAliasesClusterStateUpdateRequest() {

    }

    /**
     * Returns the alias actions to be performed
     */
    public AliasAction[] actions() {
        return actions;
    }

    /**
     * Sets the alias actions to be executed
     */
    public IndicesAliasesClusterStateUpdateRequest actions(AliasAction[] actions) {
        this.actions = actions;
        return this;
    }
}
