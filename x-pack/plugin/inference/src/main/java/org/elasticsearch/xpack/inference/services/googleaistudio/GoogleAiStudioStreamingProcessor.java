/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

class GoogleAiStudioStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, StreamingChatCompletionResults.Results> {
    private static final Logger log = LogManager.getLogger(GoogleAiStudioStreamingProcessor.class);
    private final CheckedFunction<XContentParser, String, IOException> content;

    GoogleAiStudioStreamingProcessor(CheckedFunction<XContentParser, String, IOException> content) {
        this.content = content;
    }

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = new ArrayDeque<StreamingChatCompletionResults.Result>(item.size());
        for (ServerSentEvent event : item) {
            if (event.hasData()) {
                try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, event.data())) {
                    var delta = content.apply(jsonParser);
                    results.offer(new StreamingChatCompletionResults.Result(delta));
                } catch (Exception e) {
                    log.warn("Failed to parse event from inference provider: {}", event);
                    throw e;
                }
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingChatCompletionResults.Results(results));
        }
    }
}
