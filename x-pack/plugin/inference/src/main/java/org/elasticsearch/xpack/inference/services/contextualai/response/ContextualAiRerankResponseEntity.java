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

/**
 * Parses the Contextual AI rerank response. Contextual AI rerank response looks like:
 * <pre><code>
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
 * </code></pre>
 */
public class ContextualAiRerankResponseEntity {

    /**
     * Parses the HTTP response from Contextual AI's reranking endpoint and converts it into {@link RankedDocsResults}.
     * @param response the HTTP response from Contextual AI's reranking endpoint
     * @return the parsed {@link RankedDocsResults}
     * @throws IOException if there is an error parsing the response
     */
    public static RankedDocsResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return new RankedDocsResults(doParse(jsonParser));
        }
    }

    private static List<RankedDocsResults.RankedDoc> doParse(XContentParser parser) {
        var responseObject = ResponseObject.PARSER.apply(parser, null);
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
}
