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
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Objects;

/**
 * Cluster state update request that allows to add or remove aliases
 */
public record IndicesAliasesClusterStateUpdateRequest(
    TimeValue masterNodeTimeout,
    TimeValue ackTimeout,
    List<AliasAction> actions,
    List<AliasActionResult> actionResults
) {
    public IndicesAliasesClusterStateUpdateRequest {
        Objects.requireNonNull(masterNodeTimeout);
        Objects.requireNonNull(ackTimeout);
        Objects.requireNonNull(actions);
        Objects.requireNonNull(actionResults);
    }
}
