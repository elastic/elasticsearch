/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;

public class TransportGetDatafeedsAction extends TransportMasterNodeReadAction<GetDatafeedsAction.Request, GetDatafeedsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDatafeedsAction.class);

    private final DatafeedManager datafeedManager;

    @Inject
    public TransportGetDatafeedsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DatafeedManager datafeedManager,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDatafeedsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDatafeedsAction.Request::new,
            indexNameExpressionResolver,
            GetDatafeedsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.datafeedManager = datafeedManager;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDatafeedsAction.Request request,
        ClusterState state,
        ActionListener<GetDatafeedsAction.Response> listener
    ) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        logger.debug("Get datafeed '{}'", request.getDatafeedId());

        datafeedManager.getDatafeeds(
            request,
            parentTaskId,
            listener.delegateFailureAndWrap((l, datafeeds) -> l.onResponse(new GetDatafeedsAction.Response(datafeeds)))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatafeedsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
