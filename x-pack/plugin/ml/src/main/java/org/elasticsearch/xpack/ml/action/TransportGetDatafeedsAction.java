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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportGetDatafeedsAction extends TransportMasterNodeReadAction<GetDatafeedsAction.Request, GetDatafeedsAction.Response> {

    private final DatafeedConfigProvider datafeedConfigProvider;

    @Inject
    public TransportGetDatafeedsAction(TransportService transportService,
                                       ClusterService clusterService, ThreadPool threadPool,
                                       ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       Client client, NamedXContentRegistry xContentRegistry) {
            super(GetDatafeedsAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    GetDatafeedsAction.Request::new, indexNameExpressionResolver);

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

        Map<String, DatafeedConfig> clusterStateConfigs =
                expandClusterStateDatafeeds(request.getDatafeedId(), request.allowNoDatafeeds(), state);

        datafeedConfigProvider.expandDatafeedConfigs(request.getDatafeedId(), request.allowNoDatafeeds(), ActionListener.wrap(
                datafeedBuilders -> {
                    // Check for duplicate datafeeds
                    for (DatafeedConfig.Builder datafeed : datafeedBuilders) {
                        if (clusterStateConfigs.containsKey(datafeed.getId())) {
                            listener.onFailure(new IllegalStateException("Datafeed [" + datafeed.getId() + "] configuration " +
                                    "exists in both clusterstate and index"));
                            return;
                        }
                    }

                    // Merge cluster state and index configs
                    List<DatafeedConfig> datafeeds = new ArrayList<>(datafeedBuilders.size() + clusterStateConfigs.values().size());
                    for (DatafeedConfig.Builder builder: datafeedBuilders) {
                        datafeeds.add(builder.build());
                    }

                    datafeeds.addAll(clusterStateConfigs.values());
                    Collections.sort(datafeeds, Comparator.comparing(DatafeedConfig::getId));
                    listener.onResponse(new GetDatafeedsAction.Response(new QueryPage<>(datafeeds, datafeeds.size(),
                            DatafeedConfig.RESULTS_FIELD)));
                },
                listener::onFailure
        ));
    }

    Map<String, DatafeedConfig> expandClusterStateDatafeeds(String datafeedExpression, boolean allowNoDatafeeds,
                                                            ClusterState clusterState) {

        Map<String, DatafeedConfig> configById = new HashMap<>();
        try {
            MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
            Set<String> expandedDatafeedIds = mlMetadata.expandDatafeedIds(datafeedExpression, allowNoDatafeeds);

            for (String expandedDatafeedId : expandedDatafeedIds) {
                configById.put(expandedDatafeedId, mlMetadata.getDatafeed(expandedDatafeedId));
            }
        } catch (Exception e){
            // ignore
        }

        return configById;
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatafeedsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
