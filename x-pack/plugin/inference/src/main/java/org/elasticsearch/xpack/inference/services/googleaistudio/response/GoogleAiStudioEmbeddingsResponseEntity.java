/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.XContentUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.consumeUntilObjectEnd;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class GoogleAiStudioEmbeddingsResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in Google AI Studio embeddings response";

    /**
     * Parses the Google AI Studio batch embeddings response (will be used for single and batch embeddings).
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["Embed this", "Embed this, too"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *  {
     *     "embeddings": [
     *         {
     *             "values": [
     *                 -0.00606332,
     *                 0.058092743,
     *                 -0.06390548
     *             ]
     *         },
     *         {
     *             "values": [
     *               -0.00606332,
     *               -0.06390548,
     *                0.058092743
     *             ]
     *         }
     *     ]
     *  }
     *
     *     </code>
     * </pre>
     */

    public static TextEmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<TextEmbeddingFloatResults.Embedding> embeddingList = parseList(
                jsonParser,
                GoogleAiStudioEmbeddingsResponseEntity::parseEmbeddingObject
            );

            return new TextEmbeddingFloatResults(embeddingList);
        }
    }

    private static TextEmbeddingFloatResults.Embedding parseEmbeddingObject(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "values", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<Float> embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        // parse and discard the rest of the object
        consumeUntilObjectEnd(parser);

        return TextEmbeddingFloatResults.Embedding.of(embeddingValuesList);
    }

    private GoogleAiStudioEmbeddingsResponseEntity() {}
}
