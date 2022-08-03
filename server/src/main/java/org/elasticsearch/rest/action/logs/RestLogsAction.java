/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.logs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.MapUtils;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.function.Predicate.not;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.logs.DataStreamNamingUtils.DATA_STREAM;
import static org.elasticsearch.rest.action.logs.DataStreamNamingUtils.DATA_STREAM_DATASET;
import static org.elasticsearch.rest.action.logs.DataStreamNamingUtils.DATA_STREAM_NAMESPACE;
import static org.elasticsearch.rest.action.logs.DataStreamNamingUtils.DATA_STREAM_TYPE;
import static org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY;

public class RestLogsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestLogsAction.class);

    @Override
    public String getName() {
        return "logs_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_logs"),
            new Route(POST, "/_logs/{data_stream.dataset}"),
            new Route(POST, "/_logs/{data_stream.dataset}/{data_stream.namespace}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> globalMetadata = new HashMap<>();
        Map<String, Object> localMetadata = new HashMap<>();
        Map<String, String> params = request.params();
        params.entrySet()
            .stream()
            .filter(not(e -> e.getKey().startsWith("_")))
            .forEach(e -> addPath(globalMetadata, e.getKey(), request.param(e.getKey())));

        List<IndexRequest> indexRequests = new ArrayList<>();
        XContent xContent = request.getXContentType().xContent();
        BytesReference content = request.content();
        byte separator = xContent.streamSeparator();
        for (int offset = 0, line = 0, endOfEvent; offset < content.length() - 1; offset = endOfEvent + 1, line++) {
            endOfEvent = content.indexOf(separator, offset);
            if (endOfEvent == -1) {
                endOfEvent = content.length();
            }
            try (XContentParser parser = xContent.createParser(EMPTY, content.array(), offset, endOfEvent - offset)) {
                Map<String, Object> event = null;
                try {
                    event = parser.map();
                    if (event.size() == 1 && event.containsKey("_metadata")) {
                        Map<String, Object> metadata = getMetadata(event);
                        expandDots(metadata);
                        if (line == 0) {
                            MapUtils.recursiveMerge(globalMetadata, metadata);
                        } else {
                            localMetadata = metadata;
                        }
                        continue;
                    } else {
                        expandDots(event);
                        event = mergeMetadata(event, globalMetadata, localMetadata);
                        applyDefaultValues(event);
                    }
                } catch (Exception e) {
                    event = mergeMetadata(Map.of(), globalMetadata, localMetadata);
                    event.put("message", content.slice(offset, endOfEvent - offset).utf8ToString());
                    event = createDlqDoc(event, e);
                }
                indexRequests.add(Requests.indexRequest(routeToDataStream(event)).opType(DocWriteRequest.OpType.CREATE).source(event));
            }
        }

        return channel -> {
            client.bulk(Requests.bulkRequest().add(indexRequests), new RestActionListener<>(channel) {
                @Override
                protected void processResponse(BulkResponse bulkItemResponses) throws Exception {
                    if (bulkItemResponses.hasFailures() == false) {
                        sendResponse(channel, RestStatus.ACCEPTED, b -> {});
                        return;
                    }
                    BulkRequest retryBulk = Requests.bulkRequest();
                    Arrays.stream(bulkItemResponses.getItems()).filter(BulkItemResponse::isFailed).forEach(failedRequest -> {
                        IndexRequest originalRequest = indexRequests.get(failedRequest.getItemId());
                        Map<String, Object> doc = originalRequest.sourceAsMap();
                        BulkItemResponse.Failure failure = failedRequest.getFailure();
                        if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                            // looks like a transient error; re-try as-is
                            retryBulk.add(Requests.indexRequest(originalRequest.index()).opType(DocWriteRequest.OpType.CREATE).source(doc));
                        } else {
                            // looks like a persistent error (such as a mapping issue);
                            // re-try in fallback data stream which has lenient mappings
                            Exception cause = failure.getCause();
                            doc = createDlqDoc(doc, cause);
                            retryBulk.add(Requests.indexRequest(routeToDataStream(doc)).opType(DocWriteRequest.OpType.CREATE).source(doc));
                        }
                    });
                    client.bulk(retryBulk, new RestActionListener<>(channel) {
                        @Override
                        protected void processResponse(BulkResponse bulkItemResponses) throws Exception {
                            if (bulkItemResponses.hasFailures() == false) {
                                sendResponse(channel, RestStatus.ACCEPTED, b -> {});
                            } else {
                                sendResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, b -> {});
                                logger.error(
                                    "Failed to ingest logs: re-try batch has failures. First failure: {}",
                                    Arrays.stream(bulkItemResponses.getItems())
                                        .filter(BulkItemResponse::isFailed)
                                        .findFirst()
                                        .map(BulkItemResponse::getFailureMessage)
                                        .orElse(null)
                                );
                            }
                        }
                    });
                }
            });
        };
    }

    private Map<String, Object> createDlqDoc(Map<String, Object> doc, Exception cause) {
        Map<String, Object> dlqDoc = new HashMap<>();
        dlqDoc.put("@timestamp", Instant.now().toString());
        addPath(dlqDoc, "event.original", doc);
        addPath(dlqDoc, "error.type", ElasticsearchException.getExceptionName(cause));
        addPath(dlqDoc, "error.message", cause.getMessage());
        addPath(dlqDoc, DATA_STREAM_TYPE, "logs");
        addPath(dlqDoc, DATA_STREAM_DATASET, "dlq");
        @SuppressWarnings("unchecked")
        Map<String, String> dataStream = (Map<String, String>) doc.getOrDefault(DATA_STREAM, Map.of());
        addPath(dlqDoc, DATA_STREAM_NAMESPACE, dataStream.getOrDefault("namespace", "default"));
        return dlqDoc;
    }

    private Map<String, Object> mergeMetadata(
        Map<String, Object> event,
        Map<String, Object> globalMetadata,
        Map<String, Object> localMetadata
    ) {
        HashMap<String, Object> doc = new HashMap<>(globalMetadata);
        MapUtils.recursiveMerge(doc, localMetadata);
        MapUtils.recursiveMerge(doc, event);
        return doc;
    }

    private void applyDefaultValues(Map<String, Object> doc) {
        doc.computeIfAbsent("@timestamp", k -> Instant.now().toString());
        doc.computeIfAbsent("data_stream", k -> new HashMap<>());
        @SuppressWarnings("unchecked")
        Map<String, String> dataStream = (Map<String, String>) doc.get("data_stream");
        dataStream.put("type", "logs");
        dataStream.computeIfPresent("dataset", (k, v) -> DataStreamNamingUtils.sanitizeDataStreamDataset(v));
        dataStream.computeIfPresent("namespace", (k, v) -> DataStreamNamingUtils.sanitizeDataStreamNamespace(v));
        dataStream.putIfAbsent("dataset", "generic");
        dataStream.putIfAbsent("namespace", "default");
    }

    /*
     * routing based on data_stream.* fields
     * this part will be handled by document based routing in the future
     * for example, by a routing pipeline that is attached to the logs-router-default data stream
     */
    private String routeToDataStream(Map<String, Object> doc) {
        @SuppressWarnings("unchecked")
        Map<String, String> dataStream = (Map<String, String>) doc.getOrDefault("data_stream", Map.of());
        return "logs-" + dataStream.getOrDefault("dataset", "generic") + "-" + dataStream.getOrDefault("namespace", "default");
    }

    public void sendResponse(RestChannel channel, RestStatus status, Consumer<XContentBuilder> builderConsumer) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builderConsumer.accept(builder);
            channel.sendResponse(new RestResponse(status, builder));
        }
    }

    private Map<String, Object> getMetadata(Map<String, ?> event) {
        Object metadata = event.get("_metadata");
        if (metadata instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadataMap = (Map<String, Object>) metadata;
            return metadataMap;
        }
        return Map.of();
    }

    public static void expandDots(Map<String, Object> doc) {
        for (String key : new ArrayList<>(doc.keySet())) {
            if (key.contains(".")) {
                Object value = doc.remove(key);
                addPath(doc, key, value);
            }
        }
    }

    private static void addPath(Map<String, Object> doc, String path, Object value) {
        Map<String, Object> parent = doc;
        String[] pathElements = path.split("\\.");
        for (int i = 0; i < pathElements.length - 1; i++) {
            String pathElement = pathElements[i];
            if (parent.containsKey(pathElement) == false) {
                parent.put(pathElement, new HashMap<>());
            }
            Object potentialParent = parent.get(pathElement);
            if (potentialParent instanceof Map) {
                // as this is a json object, if it's a map, it's guaranteed to be a Map<String, Object>
                // that's because there can't be non-string keys in json objects
                @SuppressWarnings("unchecked")
                Map<String, Object> mapParent = (Map<String, Object>) potentialParent;
                parent = mapParent;
            } else {
                // conflict, put the dotted key back in
                doc.put(path, value);
                return;
            }
        }
        parent.put(pathElements[pathElements.length - 1], value);
    }
}
