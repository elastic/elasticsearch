/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
  * A service that manages the persistent {@link Connector} configurations.
  *
  */
public class ConnectorIndexService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(ConnectorIndexService.class);
    public static final String CONNECTOR_ALIAS_NAME = ".elastic-connectors";
    public static final String CONNECTOR_CONCRETE_INDEX_NAME = ".elastic-connectors-v1";
    public static final String CONNECTOR_SYNC_JOB_ALIAS_NAME = ".elastic-connectors-sync-jobs";
    public static final String CONNECTOR_SYNC_JOB_CONCRETE_INDEX_NAME = ".elastic-connectors-sync-jobs-v1";
    public static final String ENT_SEARCH_INGESTION_PIPELINE_NAME = "ent-search-generic-ingestion";

    private final Client clientWithOrigin;
    private final ClusterService clusterService;
    public final NamedWriteableRegistry namedWriteableRegistry;

    public ConnectorIndexService(Client client, ClusterService clusterService, NamedWriteableRegistry namedWriteableRegistry) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.clusterService = clusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait for state recovered
            return;
        }
        ensureInternalIndex(
            this.clientWithOrigin,
            CONNECTOR_CONCRETE_INDEX_NAME,
            getConnectorsIndexMappings(),
            getConnectorsIndexSettings(),
            CONNECTOR_ALIAS_NAME
        );
        ensureInternalIndex(
            this.clientWithOrigin,
            CONNECTOR_SYNC_JOB_CONCRETE_INDEX_NAME,
            getConnectorSyncJobsIndexMappings(),
            getConnectorSyncJobsIndexSettings(),
            CONNECTOR_SYNC_JOB_ALIAS_NAME
        );

        ensurePipeline(clientWithOrigin);

        this.clusterService.removeListener(this);
    }

    private static void createInternalIndex(Client client, String index, XContentBuilder mappings, Settings settings, String alias) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(index).mapping(mappings).settings(settings).alias(new Alias(alias));
            ThreadContext threadContext = client.threadPool().getThreadContext();
            executeAsyncWithOrigin(threadContext, ENT_SEARCH_ORIGIN, request, new ActionListener<CreateIndexResponse>() {
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    logger.info("Created " + index + " index.");
                }

                public void onFailure(Exception e) {
                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof ResourceAlreadyExistsException) {
                        logger.error("Index " + index + " already exists.");
                    } else {
                        logger.error("Failed to create " + index + " index " + e.toString());
                    }
                }
            }, client.admin().indices()::create);
        } catch (Exception e) {
            logger.error("Error creating index " + index + " ." + e);
        }
    }

    private static void createPipeline(Client client) {
        BytesArray bytes = new BytesArray(EntSearchPipeline.getPipelineDefinition().getBytes());
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest(ENT_SEARCH_INGESTION_PIPELINE_NAME, bytes, XContentType.JSON);
        client.admin().cluster().putPipeline(putPipelineRequest);
        logger.info("Created pipeline" + ENT_SEARCH_INGESTION_PIPELINE_NAME);
    }

    private static void ensureInternalIndex(Client client, String index, XContentBuilder mappings, Settings settings, String alias) {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(CONNECTOR_CONCRETE_INDEX_NAME);
        ThreadContext threadContext = client.threadPool().getThreadContext();
        executeAsyncWithOrigin(threadContext, ENT_SEARCH_ORIGIN, getIndexRequest, new ActionListener<GetIndexResponse>() {
            public void onResponse(GetIndexResponse getIndexResponse) {
                logger.info("Found " + index + " index.");
            }

            public void onFailure(Exception e) {
                final Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof ResourceNotFoundException) {
                    logger.error("Index " + index + " not found.");
                    createInternalIndex(client, index, mappings, settings, alias);
                } else {
                    logger.error("Error getting " + index + " index " + e.toString());
                }
            }
        }, client.admin().indices()::getIndex);
    }

    private static void ensurePipeline(Client client) {
        GetPipelineRequest getPipelineRequest = new GetPipelineRequest(ENT_SEARCH_INGESTION_PIPELINE_NAME);
        ThreadContext threadContext = client.threadPool().getThreadContext();
        executeAsyncWithOrigin(threadContext, ENT_SEARCH_ORIGIN, getPipelineRequest, new ActionListener<>() {
            public void onResponse(GetPipelineResponse getPipelineResponse) {
                if (getPipelineResponse.isFound() == false) {
                    createPipeline(client);
                } else {
                    logger.info("Found " + ENT_SEARCH_INGESTION_PIPELINE_NAME + " pipeline.");
                }
            }

            public void onFailure(Exception e) {
                final Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof ResourceNotFoundException) {
                    logger.error("Pipeline " + ENT_SEARCH_INGESTION_PIPELINE_NAME + " not found.");
                    createPipeline(client);
                } else {
                    logger.error("Error getting " + ENT_SEARCH_INGESTION_PIPELINE_NAME + " index " + e.toString());
                }
            }
        }, client.admin().cluster()::getPipeline);
    }

    private static Settings getConnectorsIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-3")
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put("index.refresh_interval", "1s")
            .build();
    }

    private static XContentBuilder getConnectorsIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.toString());
                builder.endObject();

                builder.field("dynamic", "false");
                builder.startObject("properties");
                {
                    builder.startObject(Connector.ERROR_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(Connector.INDEX_NAME_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(Connector.IS_NATIVE_FIELD.getPreferredName());
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject(Connector.LANGUAGE_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(Connector.LAST_SEEN_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(Connector.LAST_SYNC_ERROR_FIELD.getPreferredName());
                    builder.field("type", "text");
                    builder.endObject();

                    builder.startObject(Connector.LAST_SYNC_STATUS_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(Connector.LAST_SYNC_SCHEDULED_AT_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(Connector.LAST_SYNCED_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(Connector.NAME_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(Connector.SERVICE_TYPE_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(Connector.STATUS_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(Connector.SYNC_NOW_FIELD.getPreferredName());
                    builder.field("type", "boolean");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + CONNECTOR_CONCRETE_INDEX_NAME + " index mappings", e);
            throw new UncheckedIOException("Failed to build " + CONNECTOR_CONCRETE_INDEX_NAME + " index mappings", e);
        }
    }

    private static Settings getConnectorSyncJobsIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-3")
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put("index.refresh_interval", "1s")
            .build();
    }

    private static XContentBuilder getConnectorSyncJobsIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.toString());
                builder.endObject();

                builder.field("dynamic", "false");
                builder.startObject("properties");
                {
                    builder.startObject(ConnectorSyncJob.CANCELATION_REQUESTED_AT_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.CANCELED_AT_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.COMPLETED_AT.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.CREATED_AT_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.CONNECTOR_FIELD.getPreferredName());
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject(Connector.ID_FIELD.getPreferredName());
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject(Connector.INDEX_NAME_FIELD.getPreferredName());
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject(Connector.IS_NATIVE_FIELD.getPreferredName());
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject(Connector.LANGUAGE_FIELD.getPreferredName());
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject(Connector.SERVICE_TYPE_FIELD.getPreferredName());
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.DELETED_DOCUMENT_COUNT_FIELD.getPreferredName());
                    builder.field("type", "number");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.ERROR_FIELD.getPreferredName());
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.INDEXED_DOCUMENT_COUNT_FIELD.getPreferredName());
                    builder.field("type", "number");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.INDEXED_DOCUMENT_VOLUME_FIELD.getPreferredName());
                    builder.field("type", "number");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.LAST_SEEN_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.STARTED_AT_FIELD.getPreferredName());
                    builder.field("type", "date");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.STATUS_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.TOTAL_DOCUMENT_COUNT_FIELD.getPreferredName());
                    builder.field("type", "number");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.TRIGGER_METHOD_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(ConnectorSyncJob.WORKER_HOSTNAME_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + CONNECTOR_CONCRETE_INDEX_NAME + " index mappings", e);
            throw new UncheckedIOException("Failed to build " + CONNECTOR_CONCRETE_INDEX_NAME + " index mappings", e);
        }
    }
}
