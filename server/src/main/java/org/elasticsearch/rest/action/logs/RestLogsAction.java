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
import org.elasticsearch.common.util.MapUtils;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.function.Predicate.not;
import static org.elasticsearch.rest.RestRequest.Method.POST;

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
    public boolean mediaTypesValid(RestRequest request) {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> globalMetadata = new HashMap<>();
        Map<String, String> params = request.params();
        params.entrySet()
            .stream()
            .filter(not(e -> e.getKey().startsWith("_")))
            .forEach(e -> addPath(globalMetadata, e.getKey(), request.param(e.getKey())));

        List<IndexRequest> indexRequests = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(request.content().streamInput()))) {
            Map<String, Object> localMetadata = Map.of();
            int i = 0;
            for (String line = reader.readLine(); line != null; line = reader.readLine(), i++) {
                if (line.isBlank()) {
                    continue;
                }
                Map<String, Object> event = null;
                if (line.startsWith("{")) {
                    try {
                        event = parseJson(line);
                        expandDots(event);
                    } catch (Exception e) {
                        event = new HashMap<>();
                        addPath(event, "event.original", line);
                        addPath(event, "_logs.error.type", ElasticsearchException.getExceptionName(e));
                        addPath(event, "_logs.error.message", e.getMessage());
                    }
                } else {
                    event = Map.of("message", line);
                }
                if (event.size() == 1 && event.containsKey("_metadata")) {
                    Map<String, Object> metadata = getMetadata(event);
                    expandDots(metadata);
                    if (i == 0) {
                        MapUtils.recursiveMerge(globalMetadata, metadata);
                    } else {
                        localMetadata = metadata;
                    }
                } else {
                    HashMap<String, Object> doc = new HashMap<>(globalMetadata);
                    MapUtils.recursiveMerge(doc, localMetadata);
                    MapUtils.recursiveMerge(doc, event);
                    if (doc.containsKey("@timestamp") == false) {
                        String now = Instant.now().toString();
                        doc.put("@timestamp", now);
                    }
                    // routing based on data_stream.* fields
                    // this part will be handled by document based routing in the future
                    // for example, by a routing pipeline that is attached to the logs-router-default data stream
                    doc.putIfAbsent("data_stream", new HashMap<>());
                    @SuppressWarnings("unchecked")
                    Map<String, String> dataStream = (Map<String, String>) doc.get("data_stream");
                    dataStream.putIfAbsent("type", "logs");
                    if (dataStream.get("type").equals("logs") == false) {
                        dataStream.put("dataset", "generic");
                        dataStream.put("namespace", "default");
                    } else {
                        dataStream.putIfAbsent("dataset", "generic");
                        dataStream.putIfAbsent("namespace", "default");
                    }
                    String index = "logs-" + dataStream.get("dataset") + "-" + dataStream.get("namespace");
                    indexRequests.add(Requests.indexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(doc));
                }
            }
        }

        return channel -> {
            client.bulk(Requests.bulkRequest().add(indexRequests), new RestActionListener<BulkResponse>(channel) {
                @Override
                protected void processResponse(BulkResponse bulkItemResponses) throws Exception {
                    if (bulkItemResponses.hasFailures() == false) {
                        sendResponse(channel, RestStatus.ACCEPTED, b -> {});
                        return;
                    }
                    BulkRequest retryBulk = Requests.bulkRequest();
                    Arrays.stream(bulkItemResponses.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .forEach(failedRequest -> {
                            IndexRequest originalRequest = indexRequests.get(failedRequest.getItemId());
                            Map<String, Object> doc = originalRequest.sourceAsMap();
                            BulkItemResponse.Failure failure = failedRequest.getFailure();
                            if (failure.getStatus() == RestStatus.BAD_REQUEST) {
                                // looks like an error with the document (such as a mapping issue);
                                // re-try in fallback data stream which has lenient mappings
                                Exception cause = failure.getCause();
                                addPath(doc, "_logs.error.type", ElasticsearchException.getExceptionName(cause));
                                addPath(doc, "_logs.error.message", cause.getMessage());
                                addPath(doc, "_logs.data_stream", doc.get("data_stream"));
                                addPath(doc, "_logs.data_stream", doc.get("data_stream"));
                                addPath(doc, "data_stream.dataset", "generic");
                                addPath(doc, "data_stream.namespace", "default");
                                retryBulk.add(Requests.indexRequest("logs-generic-default").opType(DocWriteRequest.OpType.CREATE).source(doc));
                            } else {
                                // looks like a transient error; re-try as-is
                                retryBulk.add(Requests.indexRequest(originalRequest.index()).opType(DocWriteRequest.OpType.CREATE).source(doc));
                            }
                        });
                    client.bulk(retryBulk, new RestActionListener<BulkResponse>(channel) {
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

    public void sendResponse(RestChannel channel, RestStatus status, Consumer<XContentBuilder> builderConsumer) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builderConsumer.accept(builder);
            channel.sendResponse(new RestResponse(status, builder));
        }
    }

    private Map<String, Object> parseJson(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.allowDuplicateKeys(true);
            return parser.map();
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> addPath(Map<String, Object> doc, String path, Object value) {
        Map<String, Object> parent = doc;
        String[] pathElements = path.split("\\.");
        for (int i = 0; i < pathElements.length; i++) {
            String pathElement = pathElements[i];
            if (i == pathElements.length - 1) {
                parent.put(pathElement, value);
            } else {
                if (parent.containsKey(pathElement) == false) {
                    parent.put(pathElement, new HashMap<>());
                }
                Object potentialParent = parent.get(pathElement);
                if (potentialParent instanceof Map) {
                    parent = (Map<String, Object>) potentialParent;
                } else {
                    // conflict, put the dotted key back in
                    doc.put(path, value);
                    break;
                }
            }
        }
        return doc;
    }
}
