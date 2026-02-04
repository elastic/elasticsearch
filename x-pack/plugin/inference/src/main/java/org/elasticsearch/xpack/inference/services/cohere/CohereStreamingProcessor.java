/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;

class CohereStreamingProcessor extends DelegatingProcessor<Deque<String>, StreamingChatCompletionResults.Results> {
    private static final Logger log = LogManager.getLogger(CohereStreamingProcessor.class);

    @Override
    protected void next(Deque<String> item) throws Exception {
        if (item.isEmpty()) {
            // discard empty result and go to the next
            upstream().request(1);
            return;
        }

        var results = new ArrayDeque<StreamingChatCompletionResults.Result>(item.size());
        for (String json : item) {
            try (var jsonParser = jsonParser(json)) {
                var responseMap = jsonParser.map();
                var eventType = (String) responseMap.get("event_type");
                switch (eventType) {
                    case "text-generation" -> parseText(responseMap).ifPresent(results::offer);
                    case "stream-end" -> validateResponse(responseMap);
                    case "stream-start", "search-queries-generation", "search-results", "citation-generation", "tool-calls-generation",
                        "tool-calls-chunk" -> {
                        log.debug("Skipping event type [{}] for line [{}].", eventType, item);
                    }
                    default -> throw new IOException("Unknown eventType found: " + eventType);
                }
            } catch (ElasticsearchStatusException e) {
                throw e;
            } catch (Exception e) {
                log.warn("Failed to parse json from cohere: {}", json);
                throw e;
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingChatCompletionResults.Results(results));
        }
    }

    private static XContentParser jsonParser(String line) throws IOException {
        return XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, line);
    }

    private Optional<StreamingChatCompletionResults.Result> parseText(Map<String, Object> responseMap) throws IOException {
        var text = (String) responseMap.get("text");
        if (text != null) {
            return Optional.of(new StreamingChatCompletionResults.Result(text));
        } else {
            throw new IOException("Null text found in text-generation cohere event");
        }
    }

    private void validateResponse(Map<String, Object> responseMap) {
        var finishReason = (String) responseMap.get("finish_reason");
        switch (finishReason) {
            case "ERROR", "ERROR_TOXIC" -> throw new ElasticsearchStatusException(
                "Cohere stopped the stream due to an error: {}",
                RestStatus.INTERNAL_SERVER_ERROR,
                parseErrorMessage(responseMap)
            );
            case "ERROR_LIMIT" -> throw new ElasticsearchStatusException(
                "Cohere stopped the stream due to an error: {}",
                RestStatus.TOO_MANY_REQUESTS,
                parseErrorMessage(responseMap)
            );
        }
    }

    @SuppressWarnings("unchecked")
    private String parseErrorMessage(Map<String, Object> responseMap) {
        var innerResponseMap = (Map<String, Object>) responseMap.get("response");
        return (String) innerResponseMap.get("text");
    }
}
