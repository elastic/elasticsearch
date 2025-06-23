/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.elastic;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class ElasticInferenceServiceDenseTextEmbeddingsResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in Elastic Inference Service dense text embeddings response";

    /**
     * Parses the Elastic Inference Service Dense Text Embeddings response.
     *
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["Embed this text", "Embed this text, too"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *             "data": [
     *                  [
     *                      2.1259406,
     *                      1.7073475,
     *                      0.9020516
     *                  ],
     *                  (...)
     *             ],
     *             "meta": {
     *                  "usage": {...}
     *             }
     *         }
     *     </code>
     * </pre>
     */

    public static TextEmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "data", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<TextEmbeddingFloatResults.Embedding> parsedEmbeddings = parseList(
                jsonParser,
                (parser, index) -> ElasticInferenceServiceDenseTextEmbeddingsResponseEntity.parseTextEmbeddingObject(parser)
            );

            if (parsedEmbeddings.isEmpty()) {
                return new TextEmbeddingFloatResults(Collections.emptyList());
            }

            return new TextEmbeddingFloatResults(parsedEmbeddings);
        }
    }

    private static TextEmbeddingFloatResults.Embedding parseTextEmbeddingObject(XContentParser parser) throws IOException {
        List<Float> embeddingValueList = parseList(
            parser,
            ElasticInferenceServiceDenseTextEmbeddingsResponseEntity::parseEmbeddingFloatValueList
        );
        return TextEmbeddingFloatResults.Embedding.of(embeddingValueList);
    }

    private static float parseEmbeddingFloatValueList(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
        return parser.floatValue();
    }

    private ElasticInferenceServiceDenseTextEmbeddingsResponseEntity() {}
}
