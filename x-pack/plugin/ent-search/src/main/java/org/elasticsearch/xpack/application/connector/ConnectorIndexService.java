/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.ToXContent;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class ConnectorIndexService {

    private final Client client;

    public static final String CONNECTOR_INDEX_NAME = ConnectorTemplateRegistry.CONNECTOR_INDEX_NAME_PATTERN;

    public ConnectorIndexService(Client client) {
        this.client = client;
    }

    private void validateConnector(Connector connector) {
        // todo: index service level validations
    }

    public void putConnector(Connector connector, ActionListener<DocWriteResponse> listener) {
        try {
            validateConnector(connector);
            final IndexRequest indexRequest = new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(connector.id())
                .opType(DocWriteRequest.OpType.INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(connector.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
            client.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
