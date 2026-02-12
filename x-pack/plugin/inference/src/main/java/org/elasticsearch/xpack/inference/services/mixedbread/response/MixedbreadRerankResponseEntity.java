/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.response;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MixedbreadRerankResponseEntity {

    /**
     * Parses the Mixedbread rerank response.

     * For a request like:
     * <pre>
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
     * </pre>
     * <p>
     *  The response will look like (without whitespace):
     *  <pre>
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
     * </pre>

     * Parses the response from a Mixedbread rerank request and returns the results.

     * @param response the http response from Mixedbread
     * @return the parsed response
     * @throws IOException if there is an error parsing the response
     */
    public static InferenceServiceResults fromResponse(HttpResult response) throws IOException {
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body())) {
            return Response.PARSER.apply(p, null).toRankedDocsResults();
        }
    }

    private record Response(List<ResultItem> results) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            Response.class.getSimpleName(),
            true,
            args -> new Response((List<ResultItem>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), ResultItem.PARSER::apply, new ParseField("data"));
        }

        public RankedDocsResults toRankedDocsResults() {
            List<RankedDocsResults.RankedDoc> rankedDocs = results.stream()
                .map(item -> new RankedDocsResults.RankedDoc(item.index(), item.relevanceScore(), item.document()))
                .toList();
            return new RankedDocsResults(rankedDocs);
        }
    }

    private record ResultItem(int index, float relevanceScore, @Nullable String document) {
        public static final ConstructingObjectParser<ResultItem, Void> PARSER = new ConstructingObjectParser<>(
            ResultItem.class.getSimpleName(),
            true,
            args -> new ResultItem((Integer) args[0], (Float) args[1], (String) args[2])
        );

        static {
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareFloat(constructorArg(), new ParseField("score"));
            PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("input"));
        }
    }
}
