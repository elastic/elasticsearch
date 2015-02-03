/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.ConfigurationService;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 */
public class IndexAlertActionFactory implements AlertActionFactory {

    private final ClientProxy client;
    private final ConfigurationService configurationService;

    public IndexAlertActionFactory(ClientProxy client, ConfigurationService configurationService){
        this.client = client;
        this.configurationService = configurationService;
    }

    @Override
    public AlertAction createAction(XContentParser parser) throws IOException {
        String index = null;
        String type = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "index":
                        index = parser.text();
                        break;
                    case "type":
                        type = parser.text();
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
            }
        }
        return new IndexAlertAction(index, type);
    }

    @Override
    public boolean doAction(AlertAction action, Alert alert, AlertsService.AlertRun alertRun) {
        if (!(action instanceof IndexAlertAction)) {
            throw new ElasticsearchIllegalStateException("Bad action [" + action.getClass() + "] passed to IndexAlertActionFactory expected [" + IndexAlertAction.class + "]");
        }

        IndexAlertAction indexAlertAction = (IndexAlertAction) action;

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(indexAlertAction.getIndex());
        indexRequest.type(indexAlertAction.getType());
        try {
            XContentBuilder resultBuilder = XContentFactory.jsonBuilder().prettyPrint();
            resultBuilder.startObject();
            resultBuilder.field("response", alertRun.data());
            resultBuilder.field("timestamp", alert.getLastExecuteTime()); ///@TODO FIXME the firetime should be in the result ?
            resultBuilder.endObject();
            indexRequest.source(resultBuilder);
        } catch (IOException ie) {
            throw new ElasticsearchException("Unable to create XContentBuilder",ie);
        }
        return client.index(indexRequest).actionGet().isCreated();
    }


}
