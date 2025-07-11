/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndices;

import java.util.Set;

public class AutoCreateDotValidator extends DotPrefixValidator<CreateIndexRequest> {
    public AutoCreateDotValidator(ThreadContext threadContext, ClusterService clusterService, SystemIndices systemIndices) {
        super(threadContext, clusterService, systemIndices);
    }

    @Override
    protected Set<String> getIndicesFromRequest(CreateIndexRequest request) {
        return Set.of(request.index());
    }

    @Override
    public String actionName() {
        return AutoCreateAction.NAME;
    }
}
