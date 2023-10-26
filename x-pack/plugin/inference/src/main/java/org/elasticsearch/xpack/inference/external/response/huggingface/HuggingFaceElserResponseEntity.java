/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class HuggingFaceElserResponseEntity {

    private static final Logger logger = LogManager.getLogger(HuggingFaceElserResponseEntity.class);

    /**
     * The response from hugging face will be formatted as [{"token": 0.0...123}]. Each object within the array will correspond to the
     * item within the inputs array within the request sent to hugging face. For example for a request like:
     *
     * <pre>
     *     <code>
     *        {
     *            "inputs": ["hello this is my name", "I wish I was there!"]
     *        }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *   <code>
     *     [
     *       {
     *         "the": 0.7226026,
     *         "to": 0.29198948,
     *         "is": 0.059944477,
     *         ...
     *       },
     *       {
     *           "wish": 0.123456,
     *           ...
     *       }
     *     ]
     *   </code>
     * </pre>
     */
    public static TextExpansionResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            if (jsonParser.currentToken() == null) {
                jsonParser.nextToken();
            }

            List<TextExpansionResults> parsedResponse = XContentParserUtils.parseList(
                jsonParser,
                HuggingFaceElserResponseEntity::parseExpansionResult
            );

            if (parsedResponse.isEmpty()) {
                return new TextExpansionResults(DEFAULT_RESULTS_FIELD, Collections.emptyList(), false);
            }

            // we only handle a single response right now so just grab the first one
            return parsedResponse.get(0);
        }
    }

    private static TextExpansionResults parseExpansionResult(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        List<TextExpansionResults.WeightedToken> weightedTokens = new ArrayList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            var floatToken = parser.nextToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, floatToken, parser);

            switch (parser.numberType()) {
                case DOUBLE -> weightedTokens.add(
                    new TextExpansionResults.WeightedToken(parser.currentName(), (float) parser.doubleValue())
                );
                case FLOAT -> weightedTokens.add(new TextExpansionResults.WeightedToken(parser.currentName(), parser.floatValue()));
                default -> {
                    logger.warn(format("Received an invalid number value in response [%s]", parser.numberType()));
                    throw new IllegalArgumentException("Expected Hugging Face Elser response to contain floats");
                }
            }
        }
        // TODO how do we know if it was truncated?
        return new TextExpansionResults(DEFAULT_RESULTS_FIELD, weightedTokens, false);
    }

    private HuggingFaceElserResponseEntity() {}
}
