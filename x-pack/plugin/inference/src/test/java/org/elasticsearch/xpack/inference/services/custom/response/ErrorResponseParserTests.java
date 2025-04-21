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
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
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

        assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: [json_parser] does not contain the required setting [path];")
        );
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
                "json_parser": {
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
        var result = getMockResult("""
            {"noerror":true}""");

        var error = ErrorMessageResponseEntity.fromResponse(result);
        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    public void testErrorResponse_ReturnsUndefinedObjectIfNotJson() throws IOException {
        var result = getMockResult("not a json string");

        var error = ErrorMessageResponseEntity.fromResponse(result);
        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    public void testParse() throws IOException {
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

        var parser = new CompletionResponseParser("$.result[*].text");
        ChatCompletionResults parsedResults = (ChatCompletionResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, is(new ChatCompletionResults(List.of(new ChatCompletionResults.Result("completion results")))));
    }

    public void testParse_AnthropicFormat() throws IOException {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "content": [
                    {
                        "type": "text",
                        "text": "result"
                    },
                    {
                        "type": "text",
                        "text": "result2"
                    }
                ],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        var parser = new CompletionResponseParser("$.content[*].text");
        ChatCompletionResults parsedResults = (ChatCompletionResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(new ChatCompletionResults(List.of(new ChatCompletionResults.Result("result"), new ChatCompletionResults.Result("result2"))))
        );
    }

    public void testParse_ThrowsException_WhenExtractedField_IsNotAList() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": "invalid_field",
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355
              }
            }
            """;

        var parser = new CompletionResponseParser("$.result[*].text");
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Current path [[*].text] matched the array field pattern "
                    + "but the current object is not a list, found invalid type [String] instead."
            )
        );
    }

    private static HttpResult getMockResult(String jsonString) throws IOException {
        var response = mock(HttpResponse.class);
        return new HttpResult(response, Strings.toUTF8Bytes(XContentHelper.stripWhitespace(jsonString)));
    }
}
