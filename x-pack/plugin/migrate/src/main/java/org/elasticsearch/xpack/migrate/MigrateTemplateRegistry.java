/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.core.template.JsonIngestPipelineConfig;

import java.util.List;

public class MigrateTemplateRegistry extends IndexTemplateRegistry {

    // This number must be incremented when we make changes to built-in pipeline.
    // If a specific user pipeline is needed instead, its version should be set to a value higher than the REGISTRY_VERSION.
    static final int REGISTRY_VERSION = 1;
    public static final String REINDEX_DATA_STREAM_PIPELINE_NAME = "reindex-data-stream-pipeline";
    private static final String TEMPLATE_VERSION_VARIABLE = "xpack.migrate.reindex.pipeline.version";

    public MigrateTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
    }

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return List.of(
            new JsonIngestPipelineConfig(
                REINDEX_DATA_STREAM_PIPELINE_NAME,
                "/" + REINDEX_DATA_STREAM_PIPELINE_NAME + ".json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        );
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.STACK_ORIGIN;
    }
}
