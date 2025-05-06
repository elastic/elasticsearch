/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser.MESSAGE_PATH;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ErrorResponseParserTests extends ESTestCase {

    public static ErrorResponseParser createRandom() {
        return new ErrorResponseParser("$." + randomAlphaOfLength(5));
    }

    public void testFromMap() {
        var validation = new ValidationException();
        var parser = ErrorResponseParser.fromMap(new HashMap<>(Map.of(MESSAGE_PATH, "$.error.message")), validation);

        assertThat(parser, is(new ErrorResponseParser("$.error.message")));
    }

    public void testFromMap_ThrowsException_WhenRequiredFieldIsNotPresent() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> ErrorResponseParser.fromMap(new HashMap<>(Map.of("some_field", "$.error.message")), validation)
        );

        assertThat(exception.getMessage(), is("Validation Failed: 1: [error_parser] does not contain the required setting [path];"));
    }

    public void testToXContent() throws IOException {
        var entity = new ErrorResponseParser("$.error.message");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        {
            builder.startObject();
            entity.toXContent(builder, null);
            builder.endObject();
        }
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "error_parser": {
                    "path": "$.error.message"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testErrorResponse_ExtractsError() throws IOException {
        var result = getMockResult("""
            {
                "error": {
                    "message": "test_error_message"
                }
            }""");

        var parser = new ErrorResponseParser("$.error.message");
        var error = parser.apply(result);
        assertThat(error, is(new ErrorResponse("test_error_message")));
    }

    public void testFromResponse_WithOtherFieldsPresent() throws IOException {
        String responseJson = """
            {
                "error": {
                    "message": "You didn't provide an API key",
                    "type": "invalid_request_error",
                    "param": null,
                    "code": null
                }
            }
            """;

        var parser = new ErrorResponseParser("$.error.message");
        var error = parser.apply(getMockResult(responseJson));

        assertThat(error, is(new ErrorResponse("You didn't provide an API key")));
    }

    public void testFromResponse_noMessage() throws IOException {
        String responseJson = """
            {
              "error": {
                "type": "not_found_error"
              }
            }
            """;

        var parser = new ErrorResponseParser("$.error.message");
        var error = parser.apply(getMockResult(responseJson));

        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
        assertThat(error.getErrorMessage(), is(""));
        assertFalse(error.errorStructureFound());
    }

    public void testErrorResponse_ReturnsUndefinedObjectIfNoError() throws IOException {
        var mockResult = getMockResult("""
            {"noerror":true}""");

        var parser = new ErrorResponseParser("$.error.message");
        var error = parser.apply(mockResult);

        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    public void testErrorResponse_ReturnsUndefinedObjectIfNotJson() {
        var result = new HttpResult(mock(HttpResponse.class), Strings.toUTF8Bytes("not a json string"));

        var parser = new ErrorResponseParser("$.error.message");
        var error = parser.apply(result);
        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    private static HttpResult getMockResult(String jsonString) throws IOException {
        var response = mock(HttpResponse.class);
        return new HttpResult(response, Strings.toUTF8Bytes(XContentHelper.stripWhitespace(jsonString)));
    }
}
