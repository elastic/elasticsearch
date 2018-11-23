/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.RunAnalyticsAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.analytics.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.analytics.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.analytics.process.AnalyticsProcessManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class TransportRunAnalyticsAction extends HandledTransportAction<RunAnalyticsAction.Request, AcknowledgedResponse> {

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final Client client;
    private final ClusterService clusterService;
    private final Environment environment;
    private final AnalyticsProcessManager analyticsProcessManager;

    /**
     * Unfortunately, getting the settings of an index include internal settings that should
     * not be set explicitly. There is no way to filter those out. Thus, we have to maintain
     * a list of them and filter them out manually.
     */
    private static final List<String> INTERNAL_SETTINGS = Arrays.asList(
        "index.creation_date",
        "index.provided_name",
        "index.uuid",
        "index.version.created"
    );

    @Inject
    public TransportRunAnalyticsAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                       Client client, ClusterService clusterService, Environment environment,
                                       AnalyticsProcessManager analyticsProcessManager) {
        super(RunAnalyticsAction.NAME, transportService, actionFilters,
            (Supplier<RunAnalyticsAction.Request>) RunAnalyticsAction.Request::new);
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
        this.environment = environment;
        this.analyticsProcessManager = analyticsProcessManager;
    }

    @Override
    protected void doExecute(Task task, RunAnalyticsAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        DiscoveryNode localNode = clusterService.localNode();
        if (isMlNode(localNode)) {
            reindexDataframeAndStartAnalysis(request.getIndex(), listener);
            return;
        }

        ClusterState clusterState = clusterService.state();
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (isMlNode(node)) {
                transportService.sendRequest(node, actionName, request,
                    new ActionListenerResponseHandler<>(listener, inputStream -> {
                            AcknowledgedResponse response = new AcknowledgedResponse();
                            response.readFrom(inputStream);
                            return response;
                    }));
                return;
            }
        }
        listener.onFailure(ExceptionsHelper.badRequestException("No ML node to run on"));
    }

    private boolean isMlNode(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String enabled = nodeAttributes.get(MachineLearning.ML_ENABLED_NODE_ATTR);
        return Boolean.valueOf(enabled);
    }

    private void reindexDataframeAndStartAnalysis(String index, ActionListener<AcknowledgedResponse> listener) {

        final String destinationIndex = index + "_copy";

        ActionListener<CreateIndexResponse> copyIndexCreatedListener = ActionListener.wrap(
            createIndexResponse -> {
                ReindexRequest reindexRequest = new ReindexRequest();
                reindexRequest.setSourceIndices(index);
                reindexRequest.setDestIndex(destinationIndex);
                client.execute(ReindexAction.INSTANCE, reindexRequest, ActionListener.wrap(
                    bulkResponse -> {
                        runPipelineAnalytics(destinationIndex, listener);
                    },
                    listener::onFailure
                ));
            }, listener::onFailure
        );

        createDestinationIndex(index, destinationIndex, copyIndexCreatedListener);
    }

    private void createDestinationIndex(String sourceIndex, String destinationIndex, ActionListener<CreateIndexResponse> listener) {
        IndexMetaData indexMetaData = clusterService.state().getMetaData().getIndices().get(sourceIndex);
        if (indexMetaData == null) {
            listener.onFailure(new IndexNotFoundException(sourceIndex));
            return;
        }

        if (indexMetaData.getMappings().size() != 1) {
            listener.onFailure(ExceptionsHelper.badRequestException("Does not support indices with multiple types"));
            return;
        }

        Settings.Builder settingsBuilder = Settings.builder().put(indexMetaData.getSettings());
        INTERNAL_SETTINGS.stream().forEach(settingsBuilder::remove);
        ImmutableOpenMap<String, MappingMetaData> mappings = indexMetaData.getMappings();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(destinationIndex, settingsBuilder.build());
        createIndexRequest.mapping(mappings.keysIt().next(), mappings.valuesIt().next().source().string(), XContentType.JSON);
        client.execute(CreateIndexAction.INSTANCE, createIndexRequest, listener);
    }

    private void runPipelineAnalytics(String index, ActionListener<AcknowledgedResponse> listener) {
        String jobId = "ml-analytics-" + index;

        ActionListener<DataFrameDataExtractorFactory> dataExtractorFactoryListener = ActionListener.wrap(
            dataExtractorFactory -> {
                DataFrameDataExtractor dataExtractor = dataExtractorFactory.newExtractor();
                analyticsProcessManager.processData(jobId, dataExtractor);
                listener.onResponse(new AcknowledgedResponse(true));
            },
            listener::onFailure
        );

        DataFrameDataExtractorFactory.create(client, Collections.emptyMap(), index, dataExtractorFactoryListener);
    }
}
