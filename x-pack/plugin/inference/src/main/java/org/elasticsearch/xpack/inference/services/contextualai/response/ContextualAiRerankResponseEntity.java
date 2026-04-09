/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

/**
 * Parses the Contextual AI rerank response.
 * Based on the API documentation, the response should look like:
 *
 * <pre>
 * {
 *   "results": [
 *     {
 *       "index": 0,
 *       "relevance_score": 0.95,
 *     },
 *     {
 *       "index": 1,
 *       "relevance_score": 0.85,
 *     }
 *   ]
 * }
 * </pre>
 */
public class ContextualAiRerankResponseEntity {

    public static RankedDocsResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);
            return new RankedDocsResults(doParse(jsonParser));
        }
    }

    private static List<RankedDocsResults.RankedDoc> doParse(XContentParser parser) {
        var responseParser = ResponseParser.PARSER;
        var responseObject = responseParser.apply(parser, null);
        return responseObject.results.stream()
            .map(result -> new RankedDocsResults.RankedDoc(result.index, result.relevanceScore, null))
            .toList();
    }

    private record ResponseObject(List<RankedDocEntry> results) {
        private static final ParseField RESULTS = new ParseField("results");
        private static final ConstructingObjectParser<ResponseObject, Void> PARSER = new ConstructingObjectParser<>(
            "contextualai_rerank_response",
            true,
            args -> {
                @SuppressWarnings("unchecked")
                List<RankedDocEntry> results = (List<RankedDocEntry>) args[0];
                return new ResponseObject(results);
            }
        );

        static {
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), RankedDocEntry.PARSER, RESULTS);
        }
    }

    private record RankedDocEntry(Integer index, Float relevanceScore) {

        private static final ParseField INDEX = new ParseField("index");
        private static final ParseField RELEVANCE_SCORE = new ParseField("relevance_score");

        private static final ConstructingObjectParser<RankedDocEntry, Void> PARSER = new ConstructingObjectParser<>(
            "contextualai_ranked_doc",
            true,
            args -> new RankedDocEntry((Integer) args[0], (Float) args[1])
        );

        static {
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), INDEX);
            PARSER.declareFloat(ConstructingObjectParser.constructorArg(), RELEVANCE_SCORE);
        }
    }

    private static class ResponseParser {
        private static final ConstructingObjectParser<ResponseObject, Void> PARSER = ResponseObject.PARSER;
    }
}
