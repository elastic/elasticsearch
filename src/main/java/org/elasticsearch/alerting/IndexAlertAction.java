/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;

public class IndexAlertAction implements AlertAction, ToXContent {
    private final String index;
    private final String type;
    private Client client = null;
    ESLogger logger = Loggers.getLogger(IndexAlertAction.class);

    @Inject
    public IndexAlertAction(String index, String type, Client client){
        this.index = index;
        this.type = type;
        this.client = client;
    }


    @Override
    public String getActionName() {
        return "index";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("type", type);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean doAction(String alertName, AlertResult alertResult) {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.type(type);
        try {
            XContentBuilder resultBuilder = XContentFactory.jsonBuilder().prettyPrint();
            resultBuilder.startObject();
            resultBuilder = alertResult.searchResponse.toXContent(resultBuilder, ToXContent.EMPTY_PARAMS);
            resultBuilder.field("timestamp", alertResult.fireTime);
            resultBuilder.endObject();
            indexRequest.source(resultBuilder);
        } catch (IOException ie) {
            throw new ElasticsearchException("Unable to create XContentBuilder",ie);
        }
        return client.index(indexRequest).actionGet().isCreated();
    }
}
