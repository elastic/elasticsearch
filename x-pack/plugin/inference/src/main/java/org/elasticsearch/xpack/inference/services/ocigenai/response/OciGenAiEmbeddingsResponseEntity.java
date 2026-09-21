/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.XContentUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class OciGenAiEmbeddingsResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in OCI Generative AI embeddings response";
    private static final String EMBEDDINGS_FIELD = "embeddings";

    /**
     * Parses the OCI Generative AI {@code embedText} response, which looks like:
     *
     * <pre>
     *     <code>
     * {
     *   "embeddings": [
     *     [ -0.018459704, 0.01399175, ... ],
     *     [ 0.030681048, 0.01714732, ... ]
     *   ],
     *   "id": "...",
     *   "modelId": "cohere.embed-v4.0",
     *   "modelVersion": "4.0",
     *   "usage": { "completionTokens": 0, "promptTokens": 4, "totalTokens": 4 }
     * }
     *     </code>
     * </pre>
     */
    public static DenseEmbeddingFloatResults fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, jsonParser.currentToken(), jsonParser);

            positionParserAtTokenAfterField(jsonParser, EMBEDDINGS_FIELD, FAILED_TO_FIND_FIELD_TEMPLATE);

            List<DenseEmbeddingFloatResults.Embedding> embeddings = parseList(jsonParser, OciGenAiEmbeddingsResponseEntity::parseEmbedding);
            return new DenseEmbeddingFloatResults(embeddings);
        }
    }

    private static DenseEmbeddingFloatResults.Embedding parseEmbedding(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<Float> values = parseList(parser, XContentUtils::parseFloat);
        return DenseEmbeddingFloatResults.Embedding.of(values);
    }

    private OciGenAiEmbeddingsResponseEntity() {}
}
