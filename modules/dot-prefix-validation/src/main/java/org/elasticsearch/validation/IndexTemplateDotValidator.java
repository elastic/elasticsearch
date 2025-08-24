/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndices;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class IndexTemplateDotValidator extends DotPrefixValidator<TransportPutComposableIndexTemplateAction.Request> {
    public IndexTemplateDotValidator(ThreadContext threadContext, ClusterService clusterService, SystemIndices systemIndices) {
        super(threadContext, clusterService, systemIndices);
    }

    @Override
    protected Set<String> getIndicesFromRequest(TransportPutComposableIndexTemplateAction.Request request) {
        return new HashSet<>(Arrays.asList(request.indices()));
    }

    @Override
    public String actionName() {
        return TransportPutComposableIndexTemplateAction.TYPE.name();
    }
}
