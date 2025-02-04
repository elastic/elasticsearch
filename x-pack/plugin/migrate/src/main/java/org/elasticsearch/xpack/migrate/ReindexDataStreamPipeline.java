/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Manages the definitions and lifecycle of the ingest pipeline used by the reindex data stream operation.
 */
public class ReindexDataStreamPipeline {

    /**
     * The last version of the distribution that updated the pipeline definition.
     */
    static final int LAST_UPDATED_VERSION = Version.V_8_18_0.id;

    public static final String PIPELINE_NAME = "reindex-data-stream";

    /**
     * Checks if the current version of the pipeline definition is installed in the cluster
     * @param clusterState The cluster state to check
     * @return true if a pipeline exists that is compatible with this version, false otherwise
     */
    public static boolean exists(ClusterState clusterState) {
        final IngestMetadata ingestMetadata = clusterState.getMetadata().custom(IngestMetadata.TYPE);
        // we ensure that we both have the pipeline and its version represents the current (or later) version
        if (ingestMetadata != null) {
            final PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(PIPELINE_NAME);
            if (pipeline != null) {
                Object version = pipeline.getConfig().get("version");
                // do not replace if pipeline was created by user and has no version
                if (version == null) {
                    return true;
                }
                return version instanceof Number number && number.intValue() >= LAST_UPDATED_VERSION;
            }
        }
        return false;
    }

    /**
     * Creates a pipeline with the current version's pipeline definition
     * @param client Client used to execute put pipeline
     * @param listener Callback used after pipeline has been created
     * @param parentTaskId parent task id so that request can be cancelled
     */
    public static void create(Client client, ActionListener<AcknowledgedResponse> listener, TaskId parentTaskId) {
        final BytesReference pipeline = BytesReference.bytes(currentPipelineDefinition());
        var putPipelineRequest = new PutPipelineRequest(
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            PIPELINE_NAME,
            pipeline,
            XContentType.JSON
        );
        putPipelineRequest.setParentTask(parentTaskId);
        client.execute(PutPipelineTransportAction.TYPE, putPipelineRequest, listener);
    }

    private static XContentBuilder currentPipelineDefinition() {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject();
            {
                builder.field(
                    "description",
                    "This pipeline sanitizes documents that are being reindexed into a data stream using the reindex data stream API. "
                        + "It is an internal pipeline and should not be modified."
                );
                builder.field("version", LAST_UPDATED_VERSION);
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("set");
                        {
                            builder.field("field", "@timestamp");
                            builder.field("value", 0);
                            builder.field("override", false);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create pipeline for reindex data stream document sanitization", e);
        }
    }
}
