/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * {@code PipelineHttpResource}s allow the checking and uploading of ingest pipelines to a remote cluster.
 * <p>
 * In the future, we will need to also support the transformation or replacement of pipelines based on their version, but we do not need
 * that functionality until some breaking change in the Monitoring API requires it.
 */
public class PipelineHttpResource extends PublishableHttpResource {

    private static final Logger logger = LogManager.getLogger(PipelineHttpResource.class);

    /**
     * The name of the pipeline that is sent to the remote cluster.
     */
    private final String pipelineName;
    /**
     * Provides a fully formed template (e.g., no variables that need replaced).
     */
    private final Supplier<byte[]> pipeline;

    /**
     * Create a new {@link PipelineHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param masterTimeout Master timeout to use with any request.
     * @param pipelineName The name of the template (e.g., ".pipeline123").
     * @param pipeline The pipeline provider.
     */
    public PipelineHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout,
                                final String pipelineName, final Supplier<byte[]> pipeline) {
        super(resourceOwnerName, masterTimeout, PublishableHttpResource.RESOURCE_VERSION_PARAMETERS);

        this.pipelineName = Objects.requireNonNull(pipelineName);
        this.pipeline = Objects.requireNonNull(pipeline);
    }

    /**
     * Determine if the current {@linkplain #pipelineName pipeline} exists.
     */
    @Override
    protected void doCheck(final RestClient client, final ActionListener<Boolean> listener) {
        versionCheckForResource(client, listener, logger,
                                "/_ingest/pipeline", pipelineName, "monitoring pipeline",
                                resourceOwnerName, "monitoring cluster",
                                XContentType.JSON.xContent(), MonitoringTemplateUtils.LAST_UPDATED_VERSION);
    }

    /**
     * Publish the current {@linkplain #pipelineName pipeline}.
     */
    @Override
    protected void doPublish(final RestClient client, final ActionListener<Boolean> listener) {
        putResource(client, listener, logger,
                    "/_ingest/pipeline", pipelineName, Collections.emptyMap(), this::pipelineToHttpEntity, "monitoring pipeline",
                    resourceOwnerName, "monitoring cluster");
    }

    /**
     * Create a {@link HttpEntity} for the {@link #pipeline}.
     *
     * @return Never {@code null}.
     */
    HttpEntity pipelineToHttpEntity() {
        return new ByteArrayEntity(pipeline.get(), ContentType.APPLICATION_JSON);
    }

}
