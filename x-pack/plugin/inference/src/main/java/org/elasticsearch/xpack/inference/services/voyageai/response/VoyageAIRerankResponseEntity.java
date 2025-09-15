/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class VoyageAIRerankResponseEntity {

    private static final Logger logger = LogManager.getLogger(VoyageAIRerankResponseEntity.class);

    record RerankResult(List<RerankResultEntry> entries) {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<RerankResult, Void> PARSER = new ConstructingObjectParser<>(
            RerankResult.class.getSimpleName(),
            true,
            args -> new RerankResult((List<RerankResultEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), RerankResultEntry.PARSER::apply, new ParseField("data"));
        }
    }

    record RerankResultEntry(Float relevanceScore, Integer index, @Nullable String document) {

        public static final ConstructingObjectParser<RerankResultEntry, Void> PARSER = new ConstructingObjectParser<>(
            RerankResultEntry.class.getSimpleName(),
            args -> new RerankResultEntry((Float) args[0], (Integer) args[1], (String) args[2])
        );

        static {
            PARSER.declareFloat(constructorArg(), new ParseField("relevance_score"));
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("document"));
        }

        public RankedDocsResults.RankedDoc toRankedDoc() {
            return new RankedDocsResults.RankedDoc(index, relevanceScore, document);
        }
    }

    /**
    * Parses the VoyageAI ranked response.
    * For a request like:
    *     "model": "rerank-2",
    *     "query": "What is the capital of the United States?",
    *     "top_k": 2,
    *     "documents": ["Carson City is the capital city of the American state of Nevada.",
    *                   "The Commonwealth of the Northern Mariana ... Its capital is Saipan.",
    *                   "Washington, D.C. (also known as simply Washington or D.C., ... It is a federal district.",
    *                   "Capital punishment (the death penalty) ... As of 2017, capital punishment is legal in 30 of the 50 states."]
    * <p>
    *  The response will look like (without whitespace):
    * {
    *   "object": "list",
    *   "data": [
    *     {
    *       "relevance_score": 0.4375,
    *       "index": 0
    *     },
    *     {
    *       "relevance_score": 0.421875,
    *       "index": 1
    *     }
    *   ],
    *   "model": "rerank-2",
    *   "usage": {
    *     "total_tokens": 26
    *   }
    * }
    * @param response the http response from VoyageAI
    * @return the parsed response
    * @throws IOException if there is an error parsing the response
    */
    public static InferenceServiceResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            var rerankResult = RerankResult.PARSER.apply(jsonParser, null);

            return new RankedDocsResults(rerankResult.entries.stream().map(RerankResultEntry::toRankedDoc).toList());
        }
    }

    private VoyageAIRerankResponseEntity() {}
}
