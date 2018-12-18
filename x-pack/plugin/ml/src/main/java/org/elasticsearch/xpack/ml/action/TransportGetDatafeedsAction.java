/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigReader;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

public class TransportGetDatafeedsAction extends TransportMasterNodeReadAction<GetDatafeedsAction.Request,
        GetDatafeedsAction.Response> {

    private final DatafeedConfigProvider datafeedConfigProvider;

    @Inject
    public TransportGetDatafeedsAction(Settings settings, TransportService transportService,
                                       ClusterService clusterService, ThreadPool threadPool,
                                       ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       Client client, NamedXContentRegistry xContentRegistry) {
        super(settings, GetDatafeedsAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, GetDatafeedsAction.Request::new);

        datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
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
                                   ActionListener<GetDatafeedsAction.Response> listener) {
        logger.debug("Get datafeed '{}'", request.getDatafeedId());

        DatafeedConfigReader datafeedConfigReader = new DatafeedConfigReader(datafeedConfigProvider);

        datafeedConfigReader.expandDatafeedConfigs(request.getDatafeedId(), request.allowNoDatafeeds(), state, ActionListener.wrap(
                datafeeds -> {
                    listener.onResponse(new GetDatafeedsAction.Response(new QueryPage<>(datafeeds, datafeeds.size(),
                            DatafeedConfig.RESULTS_FIELD)));
                },
                listener::onFailure
        ));
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatafeedsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
