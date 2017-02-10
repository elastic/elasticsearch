/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.index;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.actions.Action.Result.Status;
import org.elasticsearch.xpack.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.ArrayObjectIterator;
import org.elasticsearch.xpack.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.support.Exceptions.illegalState;

public class ExecutableIndexAction extends ExecutableAction<IndexAction> {

    public static final String ID_FIELD = "_id";

    private final WatcherClientProxy client;
    private final TimeValue timeout;

    public ExecutableIndexAction(IndexAction action, Logger logger, WatcherClientProxy client, @Nullable TimeValue defaultTimeout) {
        super(action, logger);
        this.client = client;
        this.timeout = action.timeout != null ? action.timeout : defaultTimeout;
    }

    @Override
    public Action.Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        Map<String, Object> data = payload.data();
        if (data.containsKey("_doc")) {
            Object doc = data.get("_doc");
            if (doc instanceof Iterable) {
                return indexBulk((Iterable) doc, actionId, ctx);
            }
            if (doc.getClass().isArray()) {
                return indexBulk(new ArrayObjectIterator.Iterable(doc), actionId, ctx);
            }
            if (doc instanceof Map) {
                data = (Map<String, Object>) doc;
            } else {
                throw illegalState("could not execute action [{}] of watch [{}]. failed to index payload data." +
                        "[_data] field must either hold a Map or an List/Array of Maps", actionId, ctx.watch().id());
            }
        }

        String docId = action.docId;

        // prevent double-setting id
        if (data.containsKey(ID_FIELD)) {
            if (docId != null) {
                throw illegalState("could not execute action [{}] of watch [{}]. " +
                        "[ctx.payload.{}] or [ctx.payload._doc.{}] were set with [doc_id]. Only set [{}] or [doc_id]",
                        actionId, ctx.watch().id(), ID_FIELD, ID_FIELD, ID_FIELD);
            }

            data = mutableMap(data);
            docId = data.remove(ID_FIELD).toString();
        }

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(action.index);
        indexRequest.type(action.docType);
        indexRequest.id(docId);

        data = addTimestampToDocument(data, ctx.executionTime());
        indexRequest.source(jsonBuilder().prettyPrint().map(data));

        if (ctx.simulateAction(actionId)) {
            return new IndexAction.Simulated(indexRequest.index(), action.docType, docId, new XContentSource(indexRequest.source(),
                    XContentType.JSON));
        }

        IndexResponse response = client.index(indexRequest, timeout);
        XContentBuilder jsonBuilder = jsonBuilder();
        indexResponseToXContent(jsonBuilder, response);

        return new IndexAction.Result(Status.SUCCESS, new XContentSource(jsonBuilder));
    }

    Action.Result indexBulk(Iterable list, String actionId, WatchExecutionContext ctx) throws Exception {
        if (action.docId != null) {
            throw illegalState("could not execute action [{}] of watch [{}]. [doc_id] cannot be used with bulk [_doc] indexing");
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (Object item : list) {
            if (!(item instanceof Map)) {
                throw illegalState("could not execute action [{}] of watch [{}]. failed to index payload data. " +
                        "[_data] field must either hold a Map or an List/Array of Maps", actionId, ctx.watch().id());
            }
            Map<String, Object> doc = (Map<String, Object>) item;
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index(action.index);
            indexRequest.type(action.docType);
            if (doc.containsKey(ID_FIELD)) {
                doc = mutableMap(doc);
                indexRequest.id(doc.remove(ID_FIELD).toString());
            }
            doc = addTimestampToDocument(doc, ctx.executionTime());
            indexRequest.source(jsonBuilder().prettyPrint().map(doc));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, action.timeout);
        XContentBuilder jsonBuilder = jsonBuilder().startArray();
        for (BulkItemResponse item : bulkResponse) {
            itemResponseToXContent(jsonBuilder, item);
        }
        jsonBuilder.endArray();

        // different error states, depending on how successful the bulk operation was
        long failures = Stream.of(bulkResponse.getItems()).filter(BulkItemResponse::isFailed).count();
        if (failures == 0) {
            return new IndexAction.Result(Status.SUCCESS, new XContentSource(jsonBuilder.bytes(), XContentType.JSON));
        } else if (failures == bulkResponse.getItems().length) {
            return new IndexAction.Result(Status.FAILURE, new XContentSource(jsonBuilder.bytes(), XContentType.JSON));
        } else {
            return new IndexAction.Result(Status.PARTIAL_FAILURE, new XContentSource(jsonBuilder.bytes(), XContentType.JSON));
        }
    }

    private Map<String, Object> addTimestampToDocument(Map<String, Object> data, DateTime executionTime) {
        if (action.executionTimeField != null) {
            data = mutableMap(data);
            data.put(action.executionTimeField, WatcherDateTimeUtils.formatDate(executionTime));
        }
        return data;
    }

    /**
     * Guarantees that the {@code data} is mutable for any code that needs to modify the {@linkplain Map} before using it (e.g., from
     * singleton, immutable {@code Map}s).
     *
     * @param data The map to make mutable
     * @return Always a {@linkplain HashMap}
     */
    private Map<String, Object> mutableMap(Map<String, Object> data) {
        return data instanceof HashMap ? data : new HashMap<>(data);
    }

    static void itemResponseToXContent(XContentBuilder builder, BulkItemResponse item) throws IOException {
        if (item.isFailed()) {
            builder.startObject()
                    .field("failed", item.isFailed())
                    .field("message", item.getFailureMessage())
                    .field("id", item.getId())
                    .field("type", item.getType())
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
                .field("type", response.getType())
                .field("index", response.getIndex())
                .endObject();
    }
}


