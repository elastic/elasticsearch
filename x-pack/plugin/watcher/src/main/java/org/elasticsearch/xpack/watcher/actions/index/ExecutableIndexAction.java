/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.index;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.Action.Result.Status;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.support.ArrayObjectIterator;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalState;

public class ExecutableIndexAction extends ExecutableAction<IndexAction> {

    private static final String INDEX_FIELD = "_index";
    private static final String TYPE_FIELD = "_type";
    private static final String ID_FIELD = "_id";

    private final Client client;
    private final TimeValue indexDefaultTimeout;
    private final TimeValue bulkDefaultTimeout;

    public ExecutableIndexAction(
        IndexAction action,
        Logger logger,
        Client client,
        TimeValue indexDefaultTimeout,
        TimeValue bulkDefaultTimeout
    ) {
        super(action, logger);
        this.client = client;
        this.indexDefaultTimeout = action.timeout != null ? action.timeout : indexDefaultTimeout;
        this.bulkDefaultTimeout = action.timeout != null ? action.timeout : bulkDefaultTimeout;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Action.Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        Map<String, Object> data = payload.data();
        if (data.containsKey("_doc")) {
            Object doc = data.get("_doc");
            if (doc instanceof Iterable) {
                return indexBulk((Iterable<?>) doc, actionId, ctx);
            }
            if (doc.getClass().isArray()) {
                return indexBulk(new ArrayObjectIterator.Iterable(doc), actionId, ctx);
            }
            if (doc instanceof Map) {
                data = (Map<String, Object>) doc;
            } else {
                throw illegalState(
                    "could not execute action [{}] of watch [{}]. failed to index payload data."
                        + "[_data] field must either hold a Map or an List/Array of Maps",
                    actionId,
                    ctx.watch().id()
                );
            }
        }

        if (data.containsKey(INDEX_FIELD) || data.containsKey(TYPE_FIELD) || data.containsKey(ID_FIELD)) {
            data = mutableMap(data);
        }

        IndexRequest indexRequest = new IndexRequest();
        if (action.refreshPolicy != null) {
            indexRequest.setRefreshPolicy(action.refreshPolicy);
        }

        indexRequest.index(getField(actionId, ctx.id().watchId(), "index", data, INDEX_FIELD, action.index));
        indexRequest.id(getField(actionId, ctx.id().watchId(), "id", data, ID_FIELD, action.docId));
        if (action.opType != null) {
            indexRequest.opType(action.opType);
        }

        data = addTimestampToDocument(data, ctx.executionTime());
        BytesReference bytesReference;
        try (XContentBuilder builder = jsonBuilder()) {
            indexRequest.source(builder.prettyPrint().map(data));
        }

        if (ctx.simulateAction(actionId)) {
            return new IndexAction.Simulated(
                indexRequest.index(),
                indexRequest.id(),
                action.refreshPolicy,
                new XContentSource(indexRequest.source(), XContentType.JSON)
            );
        }

        ClientHelper.assertNoAuthorizationHeader(ctx.watch().status().getHeaders());
        IndexResponse response = ClientHelper.executeWithHeaders(
            ctx.watch().status().getHeaders(),
            ClientHelper.WATCHER_ORIGIN,
            client,
            () -> client.index(indexRequest).actionGet(indexDefaultTimeout)
        );
        try (XContentBuilder builder = jsonBuilder()) {
            indexResponseToXContent(builder, response);
            bytesReference = BytesReference.bytes(builder);
        }
        return new IndexAction.Result(Status.SUCCESS, new XContentSource(bytesReference, XContentType.JSON));
    }

    Action.Result indexBulk(Iterable<?> list, String actionId, WatchExecutionContext ctx) throws Exception {
        if (action.docId != null) {
            throw illegalState("could not execute action [{}] of watch [{}]. [doc_id] cannot be used with bulk [_doc] indexing");
        }

        BulkRequest bulkRequest = new BulkRequest();
        if (action.refreshPolicy != null) {
            bulkRequest.setRefreshPolicy(action.refreshPolicy);
        }

        for (Object item : list) {
            if ((item instanceof Map) == false) {
                throw illegalState(
                    "could not execute action [{}] of watch [{}]. failed to index payload data. "
                        + "[_data] field must either hold a Map or an List/Array of Maps",
                    actionId,
                    ctx.watch().id()
                );
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) item;
            if (doc.containsKey(INDEX_FIELD) || doc.containsKey(TYPE_FIELD) || doc.containsKey(ID_FIELD)) {
                doc = mutableMap(doc);
            }

            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index(getField(actionId, ctx.id().watchId(), "index", doc, INDEX_FIELD, action.index));
            indexRequest.id(getField(actionId, ctx.id().watchId(), "id", doc, ID_FIELD, action.docId));
            if (action.opType != null) {
                indexRequest.opType(action.opType);
            }

            doc = addTimestampToDocument(doc, ctx.executionTime());
            try (XContentBuilder builder = jsonBuilder()) {
                indexRequest.source(builder.prettyPrint().map(doc));
            }
            bulkRequest.add(indexRequest);
        }

        if (ctx.simulateAction(actionId)) {
            try (XContentBuilder builder = jsonBuilder().startArray()) {
                for (DocWriteRequest<?> request : bulkRequest.requests()) {
                    builder.startObject();
                    builder.field("_id", request.id());
                    builder.field("_index", request.index());
                    builder.endObject();
                }
                builder.endArray();

                return new IndexAction.Simulated(
                    "",
                    "",
                    action.refreshPolicy,
                    new XContentSource(BytesReference.bytes(builder), XContentType.JSON)
                );
            }
        }

        ClientHelper.assertNoAuthorizationHeader(ctx.watch().status().getHeaders());
        BulkResponse bulkResponse = ClientHelper.executeWithHeaders(
            ctx.watch().status().getHeaders(),
            ClientHelper.WATCHER_ORIGIN,
            client,
            () -> client.bulk(bulkRequest).actionGet(bulkDefaultTimeout)
        );
        try (XContentBuilder jsonBuilder = jsonBuilder().startArray()) {
            for (BulkItemResponse item : bulkResponse) {
                itemResponseToXContent(jsonBuilder, item);
            }
            jsonBuilder.endArray();

            // different error states, depending on how successful the bulk operation was
            long failures = Stream.of(bulkResponse.getItems()).filter(BulkItemResponse::isFailed).count();
            if (failures == 0) {
                return new IndexAction.Result(Status.SUCCESS, new XContentSource(BytesReference.bytes(jsonBuilder), XContentType.JSON));
            } else if (failures == bulkResponse.getItems().length) {
                return new IndexAction.Result(Status.FAILURE, new XContentSource(BytesReference.bytes(jsonBuilder), XContentType.JSON));
            } else {
                return new IndexAction.Result(
                    Status.PARTIAL_FAILURE,
                    new XContentSource(BytesReference.bytes(jsonBuilder), XContentType.JSON)
                );
            }
        }
    }

    private Map<String, Object> addTimestampToDocument(Map<String, Object> data, ZonedDateTime executionTime) {
        if (action.executionTimeField != null) {
            data = mutableMap(data);
            data.put(action.executionTimeField, WatcherDateTimeUtils.formatDate(executionTime));
        }
        return data;
    }

    /**
     * Extracts the specified field out of data map, or alternative falls back to the action value
     */
    private static String getField(
        String actionId,
        String watchId,
        String name,
        Map<String, Object> data,
        String fieldName,
        String defaultValue
    ) {
        Object obj;
        // map may be immutable - only try to remove if it's actually there
        if (data.containsKey(fieldName) && (obj = data.remove(fieldName)) != null) {
            if (defaultValue != null) {
                throw illegalState(
                    "could not execute action [{}] of watch [{}]. "
                        + "[ctx.payload.{}] or [ctx.payload._doc.{}] were set together with action [{}] field. Only set one of them",
                    actionId,
                    watchId,
                    fieldName,
                    fieldName,
                    name
                );
            } else {
                return obj.toString();
            }
        }

        return defaultValue;
    }

    /**
     * Guarantees that the {@code data} is mutable for any code that needs to modify the {@linkplain Map} before using it (e.g., from
     * singleton, immutable {@code Map}s).
     *
     * @param data The map to make mutable
     * @return Always a {@linkplain HashMap}
     */
    private static Map<String, Object> mutableMap(Map<String, Object> data) {
        return data instanceof HashMap ? data : new HashMap<>(data);
    }

    private static void itemResponseToXContent(XContentBuilder builder, BulkItemResponse item) throws IOException {
        if (item.isFailed()) {
            builder.startObject()
                .field("failed", item.isFailed())
                .field("message", item.getFailureMessage())
                .field("id", item.getId())
                .field("index", item.getIndex())
                .endObject();
        } else {
            indexResponseToXContent(builder, item.getResponse());
        }
    }

    static void indexResponseToXContent(XContentBuilder builder, IndexResponse response) throws IOException {
        builder.startObject()
            .field("created", response.getResult() == DocWriteResponse.Result.CREATED)
            .field("result", response.getResult().getLowercase())
            .field("id", response.getId())
            .field("version", response.getVersion())
            .field("index", response.getIndex())
            .endObject();
    }
}
