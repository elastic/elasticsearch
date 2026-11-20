/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndices;

import java.util.Set;

public class CreateDataStreamDotValidator extends DotPrefixValidator<CreateDataStreamAction.Request> {
    public CreateDataStreamDotValidator(ThreadContext threadContext, ClusterService clusterService, SystemIndices systemIndices) {
        super(threadContext, clusterService, systemIndices);
    }

    @Override
    protected Set<String> getIndicesFromRequest(CreateDataStreamAction.Request request) {
        return Set.of(request.getName());
    }

    @Override
    public String actionName() {
        return CreateDataStreamAction.NAME;
    }
}
