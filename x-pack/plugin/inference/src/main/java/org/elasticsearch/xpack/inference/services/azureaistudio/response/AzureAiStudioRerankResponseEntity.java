/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.BaseResponseEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AzureAiStudioRerankResponseEntity extends BaseResponseEntity {
    /**
     * Parses the AzureAiStudio Search rerank json response.
     * For a request like:
     *
     * <pre>
     * <code>
     * {
     *     "model": "rerank-v3.5",
     *     "query": "What is the capital of the United States?",
     *     "top_n": 2,
     *     "documents": ["Carson City is the capital city of the American state of Nevada.",
     *                   "The Commonwealth of the Northern Mariana Islands is a group of islands in the Pacific Ocean."]
     * }
     * </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     * <code>
     * {
     *     "id": "ff2feb42-5d3a-45d7-ba29-c3dabf59988b",
     *     "results": [
     *         {
     *             "document": {
     *                 "text": "Carson City is the capital city of the American state of Nevada."
     *             },
     *             "index": 0,
     *             "relevance_score": 0.1728413
     *         },
     *         {
     *             "document": {
     *                 "text": "The Commonwealth of the Northern Mariana Islands is a group of islands in the Pacific Ocean."
     *             },
     *             "index": 1,
     *             "relevance_score": 0.031005697
     *         }
     *     ],
     *     "meta": {
     *         "api_version": {
     *             "version": "1"
     *         },
     *         "billed_units": {
     *             "search_units": 1
     *         }
     *     }
     * }
     * </code>
     * </pre>
     */
    @Override
    protected InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        final var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            var rerankResult = RerankResult.PARSER.apply(jsonParser, null);
            return new RankedDocsResults(rerankResult.entries.stream().map(RerankResultEntry::toRankedDoc).toList());
        }
    }

    record RerankResult(List<RerankResultEntry> entries) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<RerankResult, Void> PARSER = new ConstructingObjectParser<>(
            RerankResult.class.getSimpleName(),
            true,
            args -> new RerankResult((List<RerankResultEntry>) args[0])
        );
        static {
            PARSER.declareObjectArray(constructorArg(), RerankResultEntry.PARSER::apply, new ParseField("results"));
        }
    }

    record RerankResultEntry(Float relevanceScore, Integer index, @Nullable ObjectParser document) {

        public static final ConstructingObjectParser<RerankResultEntry, Void> PARSER = new ConstructingObjectParser<>(
            RerankResultEntry.class.getSimpleName(),
            args -> new RerankResultEntry((Float) args[0], (Integer) args[1], (ObjectParser) args[2])
        );
        static {
            PARSER.declareFloat(constructorArg(), new ParseField("relevance_score"));
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareObject(optionalConstructorArg(), ObjectParser.PARSER::apply, new ParseField("document"));
        }
        public RankedDocsResults.RankedDoc toRankedDoc() {
            return new RankedDocsResults.RankedDoc(index, relevanceScore, document == null ? null : document.text);
        }
    }

    record ObjectParser(String text) {
        public static final ConstructingObjectParser<ObjectParser, Void> PARSER = new ConstructingObjectParser<>(
            ObjectParser.class.getSimpleName(),
            args -> new AzureAiStudioRerankResponseEntity.ObjectParser((String) args[0])
        );
        static {
            PARSER.declareString(optionalConstructorArg(), new ParseField("text"));
        }
    }
}
