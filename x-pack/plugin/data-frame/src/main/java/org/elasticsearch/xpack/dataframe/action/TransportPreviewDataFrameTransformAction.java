/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.PreviewDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.SourceConfig;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.dataframe.transforms.DataFrameIndexer.COMPOSITE_AGGREGATION_NAME;

public class TransportPreviewDataFrameTransformAction extends
    HandledTransportAction<PreviewDataFrameTransformAction.Request, PreviewDataFrameTransformAction.Response> {

    private static final int NUMBER_OF_PREVIEW_BUCKETS = 100;
    private final XPackLicenseState licenseState;
    private final Client client;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;

    @Inject
    public TransportPreviewDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters,
                                                    Client client, ThreadPool threadPool, XPackLicenseState licenseState,
                                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                                    ClusterService clusterService) {
        super(PreviewDataFrameTransformAction.NAME,transportService, actionFilters, PreviewDataFrameTransformAction.Request::new);
        this.licenseState = licenseState;
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task,
                             PreviewDataFrameTransformAction.Request request,
                             ActionListener<PreviewDataFrameTransformAction.Response> listener) {
        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        ClusterState clusterState = clusterService.state();

        final DataFrameTransformConfig config = request.getConfig();
        for(String src : config.getSource().getIndex()) {
            String[] concreteNames = indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), src);
            if (concreteNames.length == 0) {
                listener.onFailure(new ElasticsearchStatusException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_SOURCE_INDEX_MISSING, src),
                    RestStatus.BAD_REQUEST));
                return;
            }
        }

        Pivot pivot = new Pivot(config.getPivotConfig());

        getPreview(pivot, config.getSource(), ActionListener.wrap(
            previewResponse -> listener.onResponse(new PreviewDataFrameTransformAction.Response(previewResponse)),
            listener::onFailure
        ));
    }

    private void getPreview(Pivot pivot, SourceConfig source, ActionListener<List<Map<String, Object>>> listener) {
        pivot.deduceMappings(client, source, ActionListener.wrap(
            deducedMappings -> {
                ClientHelper.executeWithHeadersAsync(threadPool.getThreadContext().getHeaders(),
                    ClientHelper.DATA_FRAME_ORIGIN,
                    client,
                    SearchAction.INSTANCE,
                    pivot.buildSearchRequest(source, null, NUMBER_OF_PREVIEW_BUCKETS),
                    ActionListener.wrap(
                        r -> {
                            final CompositeAggregation agg = r.getAggregations().get(COMPOSITE_AGGREGATION_NAME);
                            DataFrameIndexerTransformStats stats = DataFrameIndexerTransformStats.withDefaultTransformId();
                            // remove all internal fields
                            List<Map<String, Object>> results = pivot.extractResults(agg, deducedMappings, stats)
                                    .peek(record -> {
                                        record.keySet().removeIf(k -> k.startsWith("_"));
                                    }).collect(Collectors.toList());
                            listener.onResponse(results);
                        },
                        listener::onFailure
                    ));
            },
            listener::onFailure
        ));
    }
}
