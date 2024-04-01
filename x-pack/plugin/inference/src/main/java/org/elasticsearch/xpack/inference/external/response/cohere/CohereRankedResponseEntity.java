/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.cohere;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class CohereRankedResponseEntity {

    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "results", "FAILED TO FIND RESULTS FIELD"); // TODO error message

            token = jsonParser.currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                return new RankedDocsResults(parseList(jsonParser, CohereRankedResponseEntity::parseRankedDocObject));
            } else {
                throwUnknownToken(token, jsonParser);
            }

            // This should never be reached. The above code should either return successfully or hit the throwUnknownToken
            // or throw a parsing exception
            throw new IllegalStateException("Reached an invalid state while parsing the Cohere response");
        }
    }

    private static RankedDocsResults.RankedDoc parseRankedDocObject(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Integer index = null;
        Float relevanceScore = null;
        String documentText = null;
        parser.nextToken();
        while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                switch (parser.currentName()) {
                    case "index":
                        parser.nextToken(); // move to VALUE_NUMBER
                        index = parser.intValue();
                        parser.nextToken(); // move to next FIELD_NAME or END_OBJECT
                        break;
                    case "relevance_score":
                        parser.nextToken(); // move to VALUE_NUMBER
                        relevanceScore = parser.floatValue();
                        parser.nextToken(); // move to next FIELD_NAME or END_OBJECT
                        break;
                    case "document":
                        parser.nextToken(); // move to START_OBJECT; document text is wrapped in an object
                        parser.nextToken(); // move to FIELD_NAME; document text is the only field
                        parser.nextToken(); // move to VALUE_STRING; document text value
                        documentText = parser.text();
                        parser.nextToken();// move to END_OBJECT
                        parser.nextToken();// move past END_OBJECT
                        // parser should now be at the next FIELD_NAME or END_OBJECT
                        break;
                    default:
                        XContentParserUtils.throwUnknownField(parser.currentName(), parser);
                }
            }
        }
        return new RankedDocsResults.RankedDoc(String.valueOf(index), String.valueOf(relevanceScore), String.valueOf(documentText));
    }

    private CohereRankedResponseEntity() {}
}
