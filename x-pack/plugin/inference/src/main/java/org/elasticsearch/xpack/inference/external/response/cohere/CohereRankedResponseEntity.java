/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.cohere;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocs;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;

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
                return parseRerankResponseObject(jsonParser, false);
            } else {
                throwUnknownToken(token, jsonParser);
            }

            // This should never be reached. The above code should either return successfully or hit the throwUnknownToken
            // or throw a parsing exception
            throw new IllegalStateException("Reached an invalid state while parsing the Cohere response");
        }
    }

    private static InferenceServiceResults parseRerankResponseObject(XContentParser parser, boolean includesDocuments) throws IOException {
        XContentParser.Token token;

        String id;
        RankedDocs rankedDocs;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                switch (parser.currentName()) {
                    case "id":
                        parser.nextToken();
                        id = parser.text();
                        break;
                    case "results":
                        parser.nextToken();
                        rankedDocs = parseResultsArray(parser);
                        InferenceServiceResults inferenceServiceResults = new InferenceServiceResults(id);
                        return;
                        break;
                    case "meta":
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }
        throw new IllegalStateException(
            Strings.format(
                "Failed to parse response from cohere" // TODO
            )
        );
    }

    private static RankedDocs parseResultsArray(XContentParser parser) throws IOException {
        XContentParser.Token token;
        RankedDocs rankedDocs = new RankedDocs();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                RankedDocs.RankedDoc result = parseResultObject(parser);
                rankedDocs.addRankedDoc(result);
            }
        }

        return rankedDocs;
    }

    private static RankedDocs.RankedDoc parseResultObject(XContentParser parser) throws IOException {
        XContentParser.Token token;

        String documentText = null;
        int index = 0;
        double relevanceScore = 0;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                switch (parser.currentName()) {
                    case "document":
                        parser.nextToken();
                        documentText = parseDocumentObject(parser);
                        break;
                    case "index":
                        parser.nextToken();
                        index = parser.intValue();
                        break;
                    case "relevance_score":
                        parser.nextToken();
                        relevanceScore = parser.doubleValue();
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }

        // Here you should create and return an instance of Result using the parsed data
        // The exact way to do this will depend on the structure of the Result class and its constructor
        // For example:
        return new RankedDocs.RankedDoc(Integer.toString(index), relevanceScore, documentText);
    }

    private static String parseDocumentObject(XContentParser parser) throws IOException {
        XContentParser.Token token;

        String text = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                if ("text".equals(parser.currentName())) {
                    parser.nextToken();
                    text = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
        }

        return text;
    }

    private CohereRankedResponseEntity() {}
}
