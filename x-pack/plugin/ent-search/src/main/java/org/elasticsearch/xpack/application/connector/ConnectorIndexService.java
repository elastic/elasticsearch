/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xcontent.ToXContent;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class ConnectorIndexService {

    private final Client clientWithOrigin;
    private final ClusterSettings clusterSettings;

    public static final String CONNECTOR_INDEX_ALIAS_NAME = ".elastic-connectors";

    public ConnectorIndexService(Client clientWithOrigin, ClusterSettings clusterSettings) {
        this.clientWithOrigin = clientWithOrigin;
        this.clusterSettings = clusterSettings;
    }

    private void validateConnector(Connector connector) {
        // todo: index service level validations
    }

    public void putConnector(Connector connector, ActionListener<DocWriteResponse> listener) {
        try {
            validateConnector(connector);
            final IndexRequest indexRequest = new IndexRequest(CONNECTOR_INDEX_ALIAS_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(connector.id())
                .opType(DocWriteRequest.OpType.INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(connector.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void deleteConnector(String connectorId, ActionListener<DeleteResponse> listener) {
        final DeleteRequest deleteRequest = new DeleteRequest(CONNECTOR_INDEX_ALIAS_NAME).id(connectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        clientWithOrigin.delete(deleteRequest, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(new ResourceNotFoundException(connectorId));
                    return;
                }
                listener.onResponse(deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException(connectorId));
                }
                listener.onFailure(e);
            }
        });

    }
}
