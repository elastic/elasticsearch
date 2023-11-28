/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorFiltering;
import org.elasticsearch.xpack.application.connector.ConnectorIndexService;
import org.elasticsearch.xpack.application.connector.ConnectorIngestPipeline;
import org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry;
import org.elasticsearch.xpack.application.connector.syncjob.action.PostConnectorSyncJobAction;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.CONNECTORS_ORIGIN;

/**
 * A service that manages persistent {@link ConnectorSyncJob} configurations.
 */
public class ConnectorSyncJobIndexService {

    private static final Long ZERO = 0L;

    private final Client clientWithOrigin;

    public static final String CONNECTOR_SYNC_JOB_INDEX_NAME = ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN;

    /**
     * @param client A client for executing actions on the connectors sync jobs index.
     */
    public ConnectorSyncJobIndexService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, CONNECTORS_ORIGIN);
    }

    /**
     * @param request   Request for creating a connector sync job.
     * @param listener  Listener to respond to a successful response or an error.
     */
    public void createConnectorSyncJob(
        PostConnectorSyncJobAction.Request request,
        ActionListener<PostConnectorSyncJobAction.Response> listener
    ) {
        try {
            getSyncJobConnectorInfo(request.getId(), new ActionListener<>() {
                @Override
                public void onResponse(Connector connector) {
                    Instant now = Instant.now();
                    ConnectorSyncJobType jobType = Objects.requireNonNullElse(request.getJobType(), ConnectorSyncJob.DEFAULT_JOB_TYPE);
                    ConnectorSyncJobTriggerMethod triggerMethod = Objects.requireNonNullElse(
                        request.getTriggerMethod(),
                        ConnectorSyncJob.DEFAULT_TRIGGER_METHOD
                    );

                    try {
                        String syncJobId = generateId();

                        final IndexRequest indexRequest = new IndexRequest(CONNECTOR_SYNC_JOB_INDEX_NAME).id(syncJobId)
                            .opType(DocWriteRequest.OpType.INDEX)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                        ConnectorSyncJob syncJob = new ConnectorSyncJob.Builder().setId(syncJobId)
                            .setJobType(jobType)
                            .setTriggerMethod(triggerMethod)
                            .setStatus(ConnectorSyncJob.DEFAULT_INITIAL_STATUS)
                            .setConnector(connector)
                            .setCreatedAt(now)
                            .setLastSeen(now)
                            .setTotalDocumentCount(ZERO)
                            .setIndexedDocumentCount(ZERO)
                            .setIndexedDocumentVolume(ZERO)
                            .setDeletedDocumentCount(ZERO)
                            .build();

                        indexRequest.source(syncJob.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));

                        clientWithOrigin.index(
                            indexRequest,
                            ActionListener.wrap(
                                indexResponse -> listener.onResponse(new PostConnectorSyncJobAction.Response(indexResponse.getId())),
                                listener::onFailure
                            )
                        );
                    } catch (IOException e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private String generateId() {
        /* Workaround: only needed for generating an id upfront, autoGenerateId() has a side effect generating a timestamp,
         * which would raise an error on the response layer later ("autoGeneratedTimestamp should not be set externally").
         * TODO: do we even need to copy the "_id" and set it as "id"?
         */

        final IndexRequest idGeneratingRequest = new IndexRequest(CONNECTOR_SYNC_JOB_INDEX_NAME);
        idGeneratingRequest.autoGenerateId();
        return idGeneratingRequest.id();
    }

    private void getSyncJobConnectorInfo(String connectorId, ActionListener<Connector> listener) {
        try {

            final GetRequest request = new GetRequest(ConnectorIndexService.CONNECTOR_INDEX_NAME, connectorId);

            clientWithOrigin.get(request, new ActionListener<>() {
                @Override
                public void onResponse(GetResponse response) {
                    final boolean connectorDoesNotExist = response.isExists() == false;

                    if (connectorDoesNotExist) {
                        onFailure(new ResourceNotFoundException("Connector with id '" + connectorId + "' does not exist."));
                        return;
                    }

                    Map<String, Object> source = response.getSource();

                    @SuppressWarnings("unchecked")
                    final Connector syncJobConnectorInfo = new Connector.Builder().setConnectorId(
                        (String) source.get(Connector.ID_FIELD.getPreferredName())
                    )
                        .setFiltering((List<ConnectorFiltering>) source.get(Connector.FILTERING_FIELD.getPreferredName()))
                        .setIndexName((String) source.get(Connector.INDEX_NAME_FIELD.getPreferredName()))
                        .setLanguage((String) source.get(Connector.LANGUAGE_FIELD.getPreferredName()))
                        .setPipeline((ConnectorIngestPipeline) source.get(Connector.PIPELINE_FIELD.getPreferredName()))
                        .setServiceType((String) source.get(Connector.SERVICE_TYPE_FIELD.getPreferredName()))
                        .setConfiguration((Map<String, Object>) source.get(Connector.CONFIGURATION_FIELD.getPreferredName()))
                        .build();

                    listener.onResponse(syncJobConnectorInfo);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
