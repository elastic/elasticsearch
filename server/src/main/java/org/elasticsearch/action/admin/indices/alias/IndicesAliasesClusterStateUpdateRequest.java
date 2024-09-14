/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse.AliasActionResult;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.AliasAction;

import java.util.List;

/**
 * Cluster state update request that allows to add or remove aliases
 */
public class IndicesAliasesClusterStateUpdateRequest extends ClusterStateUpdateRequest<IndicesAliasesClusterStateUpdateRequest> {
    private final List<AliasAction> actions;

    private final List<IndicesAliasesResponse.AliasActionResult> actionResults;

    public IndicesAliasesClusterStateUpdateRequest(List<AliasAction> actions, List<AliasActionResult> actionResults) {
        this.actions = actions;
        this.actionResults = actionResults;
    }

    /**
     * Returns the alias actions to be performed
     */
    public List<AliasAction> actions() {
        return actions;
    }

    public List<AliasActionResult> getActionResults() {
        return actionResults;
    }
}
