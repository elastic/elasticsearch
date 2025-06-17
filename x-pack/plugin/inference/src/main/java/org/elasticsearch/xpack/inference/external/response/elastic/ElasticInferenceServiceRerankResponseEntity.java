/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.elastic;

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

public class ElasticInferenceServiceRerankResponseEntity {

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

        record RerankResultEntry(Integer index, Float relevanceScore) {

            public static final ConstructingObjectParser<RerankResultEntry, Void> PARSER = new ConstructingObjectParser<>(
                RerankResultEntry.class.getSimpleName(),
                args -> new RerankResultEntry((Integer) args[0], (Float) args[1])
            );

            static {
                PARSER.declareInt(constructorArg(), new ParseField("index"));
                PARSER.declareFloat(constructorArg(), new ParseField("relevance_score"));
            }

            public RankedDocsResults.RankedDoc toRankedDoc() {
                return new RankedDocsResults.RankedDoc(index, relevanceScore, null);
            }
        }
    }

    public static InferenceServiceResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            var rerankResult = RerankResult.PARSER.apply(jsonParser, null);

            return new RankedDocsResults(rerankResult.entries.stream().map(RerankResult.RerankResultEntry::toRankedDoc).toList());
        }
    }

    private ElasticInferenceServiceRerankResponseEntity() {}
}
