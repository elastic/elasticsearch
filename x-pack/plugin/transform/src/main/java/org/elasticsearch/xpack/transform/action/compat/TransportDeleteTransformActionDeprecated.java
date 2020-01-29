/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action.compat;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.compat.DeleteTransformActionDeprecated;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.action.TransportDeleteTransformAction;

public class TransportDeleteTransformActionDeprecated extends TransportDeleteTransformAction {

    @Inject
    public TransportDeleteTransformActionDeprecated(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformServices transformServices,
        Client client
    ) {
        super(
            DeleteTransformActionDeprecated.NAME,
            transportService,
            actionFilters,
            threadPool,
            clusterService,
            indexNameExpressionResolver,
            transformServices,
            client
        );
    }
}
