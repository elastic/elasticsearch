/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.AliasAction;

import java.util.List;

/**
 * Cluster state update request that allows to add or remove aliases
 */
public class IndicesAliasesClusterStateUpdateRequest extends ClusterStateUpdateRequest<IndicesAliasesClusterStateUpdateRequest> {
    private final List<AliasAction> actions;

    public IndicesAliasesClusterStateUpdateRequest(List<AliasAction> actions) {
        this.actions = actions;
    }

    /**
     * Returns the alias actions to be performed
     */
    public List<AliasAction> actions() {
        return actions;
    }
}
