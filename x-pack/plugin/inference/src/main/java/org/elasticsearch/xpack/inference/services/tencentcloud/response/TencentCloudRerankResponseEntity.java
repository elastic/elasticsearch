/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.response;

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

/**
 * Parses the TencentCloud rerank response, which follows the shape:
 * <pre>
 *   {
 *     "object": "list",
 *     "results": [
 *       { "index": 0, "relevance_score": 0.98, "document": "..." }
 *     ],
 *     "model": "bge-reranker-v2-m3",
 *     "usage": { "total_tokens": 45 }
 *   }
 * </pre>
 * The {@code document} field is optional and, when present, may be a plain string or an object containing a {@code text} field.
 */
public class TencentCloudRerankResponseEntity {

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
            PARSER.declareFloat(constructorArg(), new ParseField("relevance_score"));
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> parseDocument(p),
                new ParseField("document"),
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
        }
    }

    private static String parseDocument(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (token == XContentParser.Token.START_OBJECT) {
            return DocumentObject.PARSER.apply(parser, null).text();
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

    private TencentCloudRerankResponseEntity() {}
}
