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
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Predicate.not;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestLogsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestLogsAction.class);
    private final Function<String, Map<String, Object>> jsonEventMapper = logEvent -> {
        Map<String, Object> event;
        try {
            event = parseJson(logEvent);
            expandDots(event);
        } catch (Exception e) {
            event = new HashMap<>();
            addPath(event, "event.original", logEvent);
            addPath(event, "ingest.error.type", ElasticsearchException.getExceptionName(e));
            addPath(event, "ingest.error.message", e.getMessage());
        }
        return event;
    };

    private final Function<String, Map<String, Object>> rawEventMapper = logEvent -> Map.of("message", logEvent);

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
        Charset charset = Optional.ofNullable(request.getParsedContentType())
            .map(ct -> ct.getParameters().get("charset"))
            .map(Charset::forName)
            .orElse(UTF_8);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(request.content().streamInput(), charset))) {
            Map<String, Object> localMetadata = Map.of();
            int i = 0;
            String rawLogEvent = null;
            Predicate<String> isEndOfEvent = null;
            Function<String, Map<String, Object>> rawLogEventToEventObjectFunction = null;
            for (String line = reader.readLine(); line != null; line = reader.readLine(), i++) {
                if (rawLogEvent == null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    rawLogEvent = line;
                    if (line.startsWith("{")) {
                        isEndOfEvent = s -> s.endsWith("}");
                        rawLogEventToEventObjectFunction = jsonEventMapper;
                    } else {
                        isEndOfEvent = s -> true;
                        rawLogEventToEventObjectFunction = rawEventMapper;
                    }
                } else {
                    rawLogEvent += "\n" + line;
                }
                if (isEndOfEvent.test(line) == false) {
                    continue;
                }
                Map<String, Object> event = rawLogEventToEventObjectFunction.apply(rawLogEvent);
                rawLogEvent = null;

                if (event.size() == 1 && event.containsKey("_metadata")) {
                    Map<String, Object> metadata = getMetadata(event);
                    expandDots(metadata);
                    if (i == 0) {
                        MapUtils.recursiveMerge(globalMetadata, metadata);
                    } else {
                        localMetadata = metadata;
                    }
                } else {
                    addEventToBulk(globalMetadata, indexRequests, localMetadata, event);
                }
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
                            addPath(doc, "ingest.error.type", ElasticsearchException.getExceptionName(cause));
                            addPath(doc, "ingest.error.message", cause.getMessage());
                            @SuppressWarnings("unchecked")
                            Map<String, String> dataStream = (Map<String, String>) doc.get("data_stream");
                            addPath(doc, "ingest.error.data_stream", new HashMap<>(dataStream));
                            dataStream.put("type", "logs");
                            dataStream.put("dataset", "generic");
                            retryBulk.add(
                                Requests.indexRequest(routeToDataStream(dataStream)).opType(DocWriteRequest.OpType.CREATE).source(doc)
                            );
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

    private void addEventToBulk(
        Map<String, Object> globalMetadata,
        List<IndexRequest> indexRequests,
        Map<String, Object> localMetadata,
        Map<String, Object> event
    ) {
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
        dataStream.put("type", "logs");
        dataStream.putIfAbsent("dataset", "generic");
        dataStream.putIfAbsent("namespace", "default");
        indexRequests.add(
            Requests.indexRequest(routeToDataStream(dataStream)).opType(DocWriteRequest.OpType.CREATE).source(doc)
        );
    }

    private String routeToDataStream(Map<String, String> dataStream) {
        // TODO validate or sanitize dataset and namespace
        return "logs-" + dataStream.getOrDefault("dataset", "generic") + "-" + dataStream.getOrDefault("namespace", "default");
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
