/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Manages the definitions and lifecycle of the ingest pipeline used by the reindex operation within the Enrich Policy execution.
 */
public class EnrichPolicyReindexPipeline {

    /**
     * The current version of the pipeline definition. Used in the pipeline's name to differentiate from breaking changes
     * (separate from product version).
     */
    static final String CURRENT_PIPELINE_VERSION_NAME = "7";

    /**
     * The last version of the distribution that updated the pipelines definition.
     * TODO: This should be the version of ES that Enrich first ships in, which likely doesn't exist yet.
     */
    static final int ENRICH_PIPELINE_LAST_UPDATED_VERSION = Version.V_7_4_0.id;

    static String pipelineName() {
        return "enrich-policy-reindex-" + CURRENT_PIPELINE_VERSION_NAME;
    }

    /**
     * Checks if the current version of the pipeline definition is installed in the cluster
     * @param clusterState The cluster state to check
     * @return true if a pipeline exists that is compatible with this version of Enrich, false otherwise
     */
    static boolean exists(ClusterState clusterState) {
        final IngestMetadata ingestMetadata = clusterState.getMetadata().custom(IngestMetadata.TYPE);
        // we ensure that we both have the pipeline and its version represents the current (or later) version
        if (ingestMetadata != null) {
            final PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(pipelineName());
            if (pipeline != null) {
                Object version = pipeline.getConfigAsMap().get("version");
                return version instanceof Number number && number.intValue() >= ENRICH_PIPELINE_LAST_UPDATED_VERSION;
            }
        }
        return false;
    }

    /**
     * Creates a pipeline with the current version's pipeline definition
     * @param client Client used to execute put pipeline
     * @param listener Callback used after pipeline has been created
     */
    public static void create(Client client, ActionListener<AcknowledgedResponse> listener) {
        final BytesReference pipeline = BytesReference.bytes(currentEnrichPipelineDefinition(XContentType.JSON));
        final PutPipelineRequest request = new PutPipelineRequest(pipelineName(), pipeline, XContentType.JSON);
        client.admin().cluster().putPipeline(request, listener);
    }

    private static XContentBuilder currentEnrichPipelineDefinition(XContentType xContentType) {
        try {
            XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
            builder.startObject();
            {
                builder.field(
                    "description",
                    "This pipeline sanitizes documents that will be stored in enrich indices for ingest lookup "
                        + "purposes. It is an internal pipeline and should not be modified."
                );
                builder.field("version", ENRICH_PIPELINE_LAST_UPDATED_VERSION);
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        // remove the id from the document so that documents from multiple indices will always be unique.
                        builder.startObject("remove");
                        {
                            builder.field("field", "_id");
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
            throw new UncheckedIOException("Failed to create pipeline for enrich document sanitization", e);
        }
    }

}
