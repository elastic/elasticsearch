/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.response.rerank;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
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

/**
 * Nvidia Rerank Response Entity that parses the response from Nvidia's reranking model and converts it into InferenceServiceResults.
 */
public class NvidiaRerankResponseEntity {

    record RerankResult(List<RerankResultEntry> entries) {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<RerankResult, Void> PARSER = new ConstructingObjectParser<>(
            RerankResult.class.getSimpleName(),
            true,
            args -> new RerankResult((List<RerankResultEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), RerankResultEntry.PARSER::apply, new ParseField("rankings"));
        }
    }

    record RerankResultEntry(Float relevanceScore, Integer index) {

        public static final ConstructingObjectParser<RerankResultEntry, Void> PARSER = new ConstructingObjectParser<>(
            RerankResultEntry.class.getSimpleName(),
            args -> new RerankResultEntry((Float) args[0], (Integer) args[1])
        );

        static {
            PARSER.declareFloat(constructorArg(), new ParseField("logit"));
            PARSER.declareInt(constructorArg(), new ParseField("index"));
        }

        public RankedDocsResults.RankedDoc toRankedDoc() {
            return new RankedDocsResults.RankedDoc(index, relevanceScore, null);
        }
    }

    /**
     * Parses the Nvidia rerank response.
     * For a request like:
     * <pre><code>
     * {
     *     "model": "nv-rerank-qa-mistral-4b:1",
     *     "query": {
     *         "text": "cool"
     *     },
     *     "passages": [
     *         {
     *             "text": "groovy"
     *         },
     *         {
     *             "text": "boring"
     *         }
     *     ]
     * }
     * </code></pre>
     * The response will look like (without whitespace):
     * <pre><code>
     * {
     *     "rankings": [
     *         {
     *             "index": 0,
     *             "logit": 0.93017578125
     *         },
     *         {
     *             "index": 1,
     *             "logit": -9.6953125
     *         }
     *     ]
     * }
     * </code></pre>
     * @param response the http response from Nvidia
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

    private NvidiaRerankResponseEntity() {}
}
