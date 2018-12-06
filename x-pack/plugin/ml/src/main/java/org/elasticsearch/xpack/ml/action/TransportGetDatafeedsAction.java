/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TransportGetDatafeedsAction extends TransportMasterNodeReadAction<GetDatafeedsAction.Request, GetDatafeedsAction.Response> {

    @Inject
    public TransportGetDatafeedsAction(TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetDatafeedsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetDatafeedsAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDatafeedsAction.Response newResponse() {
        return new GetDatafeedsAction.Response();
    }

    @Override
    protected void masterOperation(GetDatafeedsAction.Request request, ClusterState state,
                                   ActionListener<GetDatafeedsAction.Response> listener) throws Exception {
        logger.debug("Get datafeed '{}'", request.getDatafeedId());

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(state);
        Set<String> expandedDatafeedIds = mlMetadata.expandDatafeedIds(request.getDatafeedId(), request.allowNoDatafeeds());
        List<DatafeedConfig> datafeedConfigs = new ArrayList<>();
        for (String expandedDatafeedId : expandedDatafeedIds) {
            datafeedConfigs.add(mlMetadata.getDatafeed(expandedDatafeedId));
        }

        listener.onResponse(new GetDatafeedsAction.Response(new QueryPage<>(datafeedConfigs, datafeedConfigs.size(),
                DatafeedConfig.RESULTS_FIELD)));
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatafeedsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
