/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.PreviewDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.SourceConfig;
import org.elasticsearch.xpack.dataframe.transforms.pivot.AggregationResultUtils;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.dataframe.transforms.DataFrameIndexer.COMPOSITE_AGGREGATION_NAME;

public class TransportPreviewDataFrameTransformAction extends
    HandledTransportAction<PreviewDataFrameTransformAction.Request, PreviewDataFrameTransformAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPreviewDataFrameTransformAction.class);
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
        try {
            pivot.validateConfig();
        } catch (ElasticsearchStatusException e) {
            listener.onFailure(
                new ElasticsearchStatusException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                    e.status(),
                    e));
            return;
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION, RestStatus.INTERNAL_SERVER_ERROR, e));
            return;
        }

        getPreview(pivot, config.getSource(), config.getDestination().getPipeline(), config.getDestination().getIndex(), listener);
    }

    @SuppressWarnings("unchecked")
    private void getPreview(Pivot pivot,
                            SourceConfig source,
                            String pipeline,
                            String dest,
                            ActionListener<PreviewDataFrameTransformAction.Response> listener) {
        final PreviewDataFrameTransformAction.Response previewResponse = new PreviewDataFrameTransformAction.Response();
        ActionListener<SimulatePipelineResponse> pipelineResponseActionListener = ActionListener.wrap(
            simulatePipelineResponse -> {
                List<Map<String, Object>> response = new ArrayList<>(simulatePipelineResponse.getResults().size());
                for(var simulateDocumentResult : simulatePipelineResponse.getResults()) {
                    try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
                        XContentBuilder content = simulateDocumentResult.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                        Map<String, Object> tempMap = XContentHelper.convertToMap(BytesReference.bytes(content),
                            true,
                            XContentType.JSON).v2();
                        response.add((Map<String, Object>)XContentMapValues.extractValue("doc._source", tempMap));
                    }
                }
                previewResponse.setDocs(response);
                listener.onResponse(previewResponse);
            },
            listener::onFailure
        );
        pivot.deduceMappings(client, source, ActionListener.wrap(
            deducedMappings -> {
                previewResponse.setMappingsFromStringMap(deducedMappings);
                ClientHelper.executeWithHeadersAsync(threadPool.getThreadContext().getHeaders(),
                    ClientHelper.DATA_FRAME_ORIGIN,
                    client,
                    SearchAction.INSTANCE,
                    pivot.buildSearchRequest(source, null, NUMBER_OF_PREVIEW_BUCKETS),
                    ActionListener.wrap(
                        r -> {
                            try {
                                final Aggregations aggregations = r.getAggregations();
                                if (aggregations == null) {
                                    listener.onFailure(
                                        new ElasticsearchStatusException("Source indices have been deleted or closed.",
                                            RestStatus.BAD_REQUEST)
                                    );
                                    return;
                                }
                                final CompositeAggregation agg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
                                DataFrameIndexerTransformStats stats = new DataFrameIndexerTransformStats();
                                // remove all internal fields

                                if (pipeline == null) {
                                    List<Map<String, Object>> results = pivot.extractResults(agg, deducedMappings, stats)
                                        .peek(doc -> doc.keySet().removeIf(k -> k.startsWith("_")))
                                        .collect(Collectors.toList());
                                    previewResponse.setDocs(results);
                                    listener.onResponse(previewResponse);
                                } else {
                                    List<Map<String, Object>> results = pivot.extractResults(agg, deducedMappings, stats)
                                        .map(doc -> {
                                            Map<String, Object> src = new HashMap<>();
                                            String id = (String) doc.get(DataFrameField.DOCUMENT_ID_FIELD);
                                            doc.keySet().removeIf(k -> k.startsWith("_"));
                                            src.put("_source", doc);
                                            src.put("_id", id);
                                            src.put("_index", dest);
                                            return src;
                                        }).collect(Collectors.toList());
                                    try (XContentBuilder builder = jsonBuilder()) {
                                        builder.startObject();
                                        builder.field("docs", results);
                                        builder.endObject();
                                        var pipelineRequest = new SimulatePipelineRequest(BytesReference.bytes(builder), XContentType.JSON);
                                        pipelineRequest.setId(pipeline);
                                        ClientHelper.executeAsyncWithOrigin(client,
                                            ClientHelper.DATA_FRAME_ORIGIN,
                                            SimulatePipelineAction.INSTANCE,
                                            pipelineRequest,
                                            pipelineResponseActionListener);
                                    }
                                }
                            } catch (AggregationResultUtils.AggregationExtractionException extractionException) {
                                listener.onFailure(
                                        new ElasticsearchStatusException(extractionException.getMessage(), RestStatus.BAD_REQUEST));
                            }
                        },
                        listener::onFailure
                    ));
            },
            listener::onFailure
        ));
    }
}
