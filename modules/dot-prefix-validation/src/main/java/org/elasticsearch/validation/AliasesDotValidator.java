/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndices;

import java.util.HashSet;
import java.util.Set;

/**
 * Applies the dot-prefix rule to the new name of an index that is renamed, since a rename gives an index a name as creating one does.
 * Only the new name is checked: the aliases of the other actions of the request are not index names, and the current name of an
 * index being renamed is the name it already has.
 */
public class AliasesDotValidator extends DotPrefixValidator<IndicesAliasesRequest> {

    public AliasesDotValidator(ThreadContext threadContext, ClusterService clusterService, SystemIndices systemIndices) {
        super(threadContext, clusterService, systemIndices);
    }

    @Override
    protected Set<String> getIndicesFromRequest(IndicesAliasesRequest request) {
        final Set<String> newNames = new HashSet<>();
        for (IndicesAliasesRequest.AliasActions action : request.getAliasActions()) {
            if (action.actionType() == IndicesAliasesRequest.AliasActions.Type.RENAME_INDEX) {
                newNames.add(action.newIndex());
            }
        }
        return newNames;
    }

    @Override
    public String actionName() {
        return TransportIndicesAliasesAction.NAME;
    }
}
