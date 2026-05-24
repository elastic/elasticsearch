/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.huggingface.request.rerank.HuggingFaceRerankRequest;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class HuggingFaceRerankResponseEntity {

    /**
     * Parses the Hugging Face rerank response.

     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *              "texts": ["luke", "leia"],
     *              "query": "star wars main character",
     *              "return_text": true
     *          }
     *     </code>
     * </pre>

     * The response would look like:

     * <pre>
     *     <code>
     *         [
     *              {
     *                   "index": 0,
     *                   "score": -0.07996220886707306,
     *                   "text": "luke"
     *               },
     *               {
     *                  "index": 1,
     *                  "score": -0.08393221348524094,
     *                  "text": "leia"
     *              }
     *         ]
     *     </code>
     * </pre>
     */

    public static RankedDocsResults fromResponse(HuggingFaceRerankRequest request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);
            var rankedDocs = doParse(jsonParser);
            var rankedDocsByRelevanceStream = rankedDocs.stream()
                .sorted(Comparator.comparingDouble(RankedDocsResults.RankedDoc::relevanceScore).reversed());
            var rankedDocStreamTopN = request.getTopN() == null
                ? rankedDocsByRelevanceStream
                : rankedDocsByRelevanceStream.limit(request.getTopN());
            return new RankedDocsResults(rankedDocStreamTopN.toList());
        }
    }

    private static List<RankedDocsResults.RankedDoc> doParse(XContentParser parser) throws IOException {
        return parseList(parser, (listParser, index) -> {
            var parsedRankedDoc = HuggingFaceRerankResponseEntity.RankedDocEntry.parse(parser);
            return new RankedDocsResults.RankedDoc(parsedRankedDoc.index, parsedRankedDoc.score, parsedRankedDoc.text);
        });
    }

    private record RankedDocEntry(Integer index, Float score, @Nullable String text) {

        private static final ParseField TEXT = new ParseField("text");
        private static final ParseField SCORE = new ParseField("score");
        private static final ParseField INDEX = new ParseField("index");
        private static final ConstructingObjectParser<HuggingFaceRerankResponseEntity.RankedDocEntry, Void> PARSER =
            new ConstructingObjectParser<>(
                "hugging_face_rerank_response",
                true,
                args -> new HuggingFaceRerankResponseEntity.RankedDocEntry((int) args[0], (float) args[1], (String) args[2])
            );

        static {
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), INDEX);
            PARSER.declareFloat(ConstructingObjectParser.constructorArg(), SCORE);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TEXT);
        }

        public static RankedDocEntry parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }
}
