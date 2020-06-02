/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action.compat;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.compat.StopTransformActionDeprecated;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.action.TransportStopTransformAction;

public class TransportStopTransformActionDeprecated extends TransportStopTransformAction {

    @Inject
    public TransportStopTransformActionDeprecated(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        PersistentTasksService persistentTasksService,
        TransformServices transformServices,
        Client client
    ) {
        super(
            StopTransformActionDeprecated.NAME,
            transportService,
            actionFilters,
            clusterService,
            threadPool,
            persistentTasksService,
            transformServices,
            client
        );
    }

}
