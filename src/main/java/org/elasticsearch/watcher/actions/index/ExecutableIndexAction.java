/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.watcher.actions.ExecutableAction;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExecutableIndexAction extends ExecutableAction<IndexAction, IndexAction.Result> {

    private final ClientProxy client;

    public ExecutableIndexAction(IndexAction action, ESLogger logger, ClientProxy client) {
        super(action, logger);
        this.client = client;
    }

    @Override
    protected IndexAction.Result doExecute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(action.index);
        indexRequest.type(action.docType);

        XContentBuilder resultBuilder = XContentFactory.jsonBuilder().prettyPrint();
        resultBuilder.startObject();
        resultBuilder.field("data", payload.data());
        resultBuilder.field("timestamp", ctx.executionTime());
        resultBuilder.endObject();
        indexRequest.source(resultBuilder);

        Map<String, Object> data = new HashMap<>();
        if (ctx.simulateAction(actionId)) {
            return new IndexAction.Result.Simulated(action.index, action.docType, new Payload.Simple(indexRequest.sourceAsMap()));
        }

        IndexResponse response = client.index(indexRequest);
        data.put("created", response.isCreated());
        data.put("id", response.getId());
        data.put("version", response.getVersion());
        data.put("type", response.getType());
        data.put("index", response.getIndex());
        return new IndexAction.Result.Executed(new Payload.Simple(data), response.isCreated());
    }

    @Override
    protected IndexAction.Result failure(String reason) {
        return new IndexAction.Result.Failure(reason);
    }
}


