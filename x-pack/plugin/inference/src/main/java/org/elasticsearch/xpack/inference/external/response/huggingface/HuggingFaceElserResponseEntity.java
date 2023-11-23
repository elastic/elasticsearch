/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

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

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class HuggingFaceElserResponseEntity {

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
    public static List<TextExpansionResults> fromResponse(HttpResult response) throws IOException {
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
                return List.of(new TextExpansionResults(DEFAULT_RESULTS_FIELD, Collections.emptyList(), false));
            }

            // we only handle a single response right now so just grab the first one
            return parsedResponse;
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

            weightedTokens.add(new TextExpansionResults.WeightedToken(parser.currentName(), parser.floatValue()));
        }
        // TODO how do we know if the tokens were truncated so we can set this appropriately?
        // This will depend on whether we handle the tokenization or hugging face
        return new TextExpansionResults(DEFAULT_RESULTS_FIELD, weightedTokens, false);
    }

    private HuggingFaceElserResponseEntity() {}
}
