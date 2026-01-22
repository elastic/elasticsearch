/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.response;

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

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class MixedbreadRerankResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Mixedbread rerank response";

    /**
     * Parses the Mixedbread rerank response.

     * For a request like:
     *{
     *   "model": "mixedbread-ai/mxbai-rerank-xsmall-v1",
     *   "query": "Who is the author of To Kill a Mockingbird?",
     *   "input": [
     *         "To Kill a Mockingbird is a novel by Harper Lee",
     *         "The novel Moby-Dick was written by Herman Melville",
     *         "Harper Lee, an American novelist",
     *         "Jane Austen was an English novelist",
     *         "The Harry Potter series written by British author J.K. Rowling",
     *         "The Great Gatsby, a novel written by American author F. Scott Fitzgerald"
     *     ],
     *   "top_k": 3,
     *   "return_input": false
     * }
     * <p>
     *  The response will look like (without whitespace):
     *{
     *     "usage": {
     *         "prompt_tokens": 162,
     *         "total_tokens": 162,
     *         "completion_tokens": 0
     *     },
     *     "model": "mixedbread-ai/mxbai-rerank-xsmall-v1",
     *     "data": [
     *         {
     *             "index": 0,
     *             "score": 0.98291015625,
     *             "input": null,
     *             "object": "rank_result"
     *         },
     *         {
     *             "index": 2,
     *             "score": 0.61962890625,
     *             "input": null,
     *             "object": "rank_result"
     *         },
     *         {
     *             "index": 3,
     *             "score": 0.3642578125,
     *             "input": null,
     *             "object": "rank_result"
     *         }
     *     ],
     *     "object": "list",
     *     "top_k": 3,
     *     "return_input": false
     * }

     * Parses the response from a Mixedbread rerank request and returns the results.

     * @param response the http response from Mixedbread
     * @return the parsed response
     * @throws IOException if there is an error parsing the response
     */
    public static InferenceServiceResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);
            moveToFirstToken(jsonParser);
            return new RankedDocsResults(doParse(jsonParser));
        }
    }

    private static List<RankedDocsResults.RankedDoc> doParse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        positionParserAtTokenAfterField(parser, "data", FAILED_TO_FIND_FIELD_TEMPLATE);

        token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            return parseList(parser, (listParser, index) -> {
                var parsedRankedDoc = RankedDocEntry.parse(parser);
                return new RankedDocsResults.RankedDoc(parsedRankedDoc.index, parsedRankedDoc.score, parsedRankedDoc.text);
            });
        } else {
            throwUnknownToken(token, parser);
        }

        // This should never be reached. The above code should either return successfully or hit the throwUnknownToken
        // or throw a parsing exception
        throw new IllegalStateException("Reached an invalid state while parsing the Mixedbread response");
    }

    private record RankedDocEntry(Integer index, Float score, @Nullable String text) {

        private static final ParseField TEXT = new ParseField("input");
        private static final ParseField SCORE = new ParseField("score");
        private static final ParseField INDEX = new ParseField("index");
        private static final ConstructingObjectParser<RankedDocEntry, Void> PARSER = new ConstructingObjectParser<>(
            "mixedbread_rerank_response",
            true,
            args -> new RankedDocEntry((int) args[0], (float) args[1], (String) args[2])
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
