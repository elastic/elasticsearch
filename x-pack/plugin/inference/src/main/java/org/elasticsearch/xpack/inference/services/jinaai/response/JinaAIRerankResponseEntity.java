/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.response;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class JinaAIRerankResponseEntity {

    /**
     * Parses the JinaAI ranked response.
     *
     * For a request like:
     *     "model": "jina-reranker-v2-base-multilingual",
     *     "query": "What is the capital of the United States?",
     *     "top_n": 3,
     *     "documents": ["Carson City is the capital city of the American state of Nevada.",
     *                   "The Commonwealth of the Northern Mariana ... Its capital is Saipan.",
     *                   "Washington, D.C. (also known as simply Washington or D.C., ... It is a federal district.",
     *                   "Capital punishment (the death penalty) ... As of 2017, capital punishment is legal in 30 of the 50 states."]
     * <p>
     *  The response will look like (without whitespace):
     *  <pre>
     *     {
     *     "id": "1983d114-a6e8-4940-b121-eb4ac3f6f703",
     *     "results": [
     *         {
     *             "document": {
     *                 "text": "Washington, D.C.  is the capital of the United States. It is a federal district."
     *             },
     *             "index": 2,
     *             "relevance_score": 0.98005307
     *         },
     *         {
     *             "document": {
     *                 "text": "Capital punishment (the death penalty) As of 2017, capital punishment is legal in 30 of the 50 states."
     *             },
     *             "index": 3,
     *             "relevance_score": 0.27904198
     *         },
     *         {
     *             "document": {
     *                 "text": "Carson City is the capital city of the American state of Nevada."
     *             },
     *             "index": 0,
     *             "relevance_score": 0.10194652
     *         }
     *     ],
     *     "usage": {"total_tokens": 15}
     *     }
     *  </pre>
     *
     *  Or like this where documents is a string:
     *  <pre>
     *      {
     *      "id": "1983d114-a6e8-4940-b121-eb4ac3f6f703",
     *      "results": [
     *           {
     *                "document": "Washington, D.C.  is the capital of the United States. It is a federal district.",
     *                "index": 2,
     *                "relevance_score": 0.98005307
     *           },
     *           {
     *                "document":  "abc",
     *                "index": 3,
     *                "relevance_score": 0.27904198
     *           },
     *           {
     *                "document": "Carson City is the capital city of the American state of Nevada.",
     *                "index": 0,
     *                "relevance_score": 0.10194652
     *           }
     *      ],
     *      "usage": {
     *           "total_tokens": 15
     *      }
     * }
     *  </pre>
     *
     *  This parsing logic handles both cases.
     *
     * @param response the http response from JinaAI
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
            PARSER.declareObjectArray(constructorArg(), ResultItem.PARSER::apply, new ParseField("results"));
        }

        public RankedDocsResults toRankedDocsResults() {
            List<RankedDocsResults.RankedDoc> rankedDocs = results.stream()
                .map(
                    item -> new RankedDocsResults.RankedDoc(
                        item.index(),
                        item.relevanceScore(),
                        item.document() != null ? item.document().text() : null
                    )
                )
                .toList();
            return new RankedDocsResults(rankedDocs);
        }
    }

    private record ResultItem(int index, float relevanceScore, @Nullable Document document) {
        public static final ConstructingObjectParser<ResultItem, Void> PARSER = new ConstructingObjectParser<>(
            ResultItem.class.getSimpleName(),
            true,
            args -> new ResultItem((Integer) args[0], (Float) args[1], (Document) args[2])
        );

        static {
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareFloat(constructorArg(), new ParseField("relevance_score"));
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> parseDocument(p),
                new ParseField("document"),
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
        }
    }

    private record Document(String text) {}

    private static Document parseDocument(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            return new Document(DocumentObject.PARSER.apply(parser, null).text());
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return new Document(parser.text());
        }

        throw new XContentParseException(parser.getTokenLocation(), "Expected an object or string for document field, but got: " + token);
    }

    private record DocumentObject(String text) {
        public static final ConstructingObjectParser<DocumentObject, Void> PARSER = new ConstructingObjectParser<>(
            DocumentObject.class.getSimpleName(),
            true,
            args -> new DocumentObject((String) args[0])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("text"));
        }
    }
}
