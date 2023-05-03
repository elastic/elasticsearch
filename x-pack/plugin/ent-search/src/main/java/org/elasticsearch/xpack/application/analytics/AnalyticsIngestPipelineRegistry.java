/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.application.utils.ingest.PipelineRegistry;
import org.elasticsearch.xpack.application.utils.ingest.PipelineTemplateConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PREFIX;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.ROOT_RESOURCE_PATH;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.TEMPLATE_VERSION_VARIABLE;
import static org.elasticsearch.xpack.application.analytics.AnalyticsTemplateRegistry.REGISTRY_VERSION;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * Set up the behavioral analytics ingest pipelines.
 */
public class AnalyticsIngestPipelineRegistry extends PipelineRegistry {

    // Ingest pipelines configuration.
    static final String EVENT_DATA_STREAM_INGEST_PIPELINE_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "final_pipeline";
    static final List<PipelineTemplateConfiguration> INGEST_PIPELINES = Collections.singletonList(
        new PipelineTemplateConfiguration(
            EVENT_DATA_STREAM_INGEST_PIPELINE_NAME,
            ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_INGEST_PIPELINE_NAME + ".json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE
        )
    );

    public AnalyticsIngestPipelineRegistry(ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(clusterService, threadPool, client);
    }

    @Override
    protected String getOrigin() {
        return ENT_SEARCH_ORIGIN;
    }

    @Override
    protected List<PipelineTemplateConfiguration> getIngestPipelineConfigs() {
        return INGEST_PIPELINES;
    }

    @Override
    protected boolean isClusterReady(ClusterChangedEvent event) {
        return super.isClusterReady(event) && (isIngestPipelineInstalled(event.state()) || hasAnalyticsEventDataStream(event.state()));
    }

    private boolean hasAnalyticsEventDataStream(ClusterState state) {
        Set<String> dataStreamNames = state.metadata().dataStreams().keySet();

        return dataStreamNames.stream().anyMatch(dataStreamName -> dataStreamName.startsWith(EVENT_DATA_STREAM_INDEX_PREFIX));
    }

    private boolean isIngestPipelineInstalled(ClusterState state) {
        return ingestPipelineExists(state, EVENT_DATA_STREAM_INGEST_PIPELINE_NAME);
    }
}
