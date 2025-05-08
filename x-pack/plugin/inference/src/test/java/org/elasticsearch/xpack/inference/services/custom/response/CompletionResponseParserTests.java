/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser.COMPLETION_PARSER_RESULT;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CompletionResponseParserTests extends AbstractBWCWireSerializationTestCase<CompletionResponseParser> {

    public static CompletionResponseParser createRandom() {
        return new CompletionResponseParser("$." + randomAlphaOfLength(5));
    }

    public void testFromMap() {
        var validation = new ValidationException();

        var parser = CompletionResponseParser.fromMap(
            new HashMap<>(Map.of(COMPLETION_PARSER_RESULT, "$.result[*].text")),
            "scope",
            validation
        );

        assertThat(parser, is(new CompletionResponseParser("$.result[*].text")));
    }

    public void testFromMap_ThrowsException_WhenRequiredFieldIsNotPresent() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> CompletionResponseParser.fromMap(new HashMap<>(Map.of("some_field", "$.result[*].text")), "scope", validation)
        );

        assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: [scope.json_parser] does not contain the required setting [completion_result];")
        );
    }

    public void testToXContent() throws IOException {
        var entity = new CompletionResponseParser("$.result[*].text");

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
                    "completion_result": "$.result[*].text"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testParse() throws IOException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": [
                  {
                    "text":"completion results"
                  }
              ],
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355
              }
            }
            """;

        var parser = new CompletionResponseParser("$.result[*].text");
        ChatCompletionResults parsedResults = (ChatCompletionResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, is(new ChatCompletionResults(List.of(new ChatCompletionResults.Result("completion results")))));
    }

    public void testParse_String() throws IOException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": {
                "text":"completion results"
              },
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355
              }
            }
            """;

        var parser = new CompletionResponseParser("$.result.text");
        ChatCompletionResults parsedResults = (ChatCompletionResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, is(new ChatCompletionResults(List.of(new ChatCompletionResults.Result("completion results")))));
    }

    public void testParse_MultipleResults() throws IOException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": [
                  {
                    "text":"completion results"
                  },
                  {
                    "text":"completion results2"
                  }
              ],
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355
              }
            }
            """;

        var parser = new CompletionResponseParser("$.result[*].text");
        ChatCompletionResults parsedResults = (ChatCompletionResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new ChatCompletionResults(
                    List.of(new ChatCompletionResults.Result("completion results"), new ChatCompletionResults.Result("completion results2"))
                )
            )
        );
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

    public void testParse_ThrowsException_WhenExtractedField_IsNotListOfStrings() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": ["string", true],
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355
              }
            }
            """;

        var parser = new CompletionResponseParser("$.result");
        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is("Failed to parse list entry [1], error: Unable to convert field [$.result] of type [Boolean] to [String]")
        );
    }

    public void testParse_ThrowsException_WhenExtractedField_IsNotAListOrString() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": 123,
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355
              }
            }
            """;

        var parser = new CompletionResponseParser("$.result");
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is("Extracted field [result] from path [$.result] is an invalid type, expected a list or a string but received [Integer]")
        );
    }

    @Override
    protected CompletionResponseParser mutateInstanceForVersion(CompletionResponseParser instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<CompletionResponseParser> instanceReader() {
        return CompletionResponseParser::new;
    }

    @Override
    protected CompletionResponseParser createTestInstance() {
        return createRandom();
    }

    @Override
    protected CompletionResponseParser mutateInstance(CompletionResponseParser instance) throws IOException {
        return randomValueOtherThan(instance, CompletionResponseParserTests::createRandom);
    }
}
