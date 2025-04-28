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
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser.TEXT_EMBEDDING_PARSER_EMBEDDINGS;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TextEmbeddingResponseParserTests extends AbstractBWCWireSerializationTestCase<TextEmbeddingResponseParser> {

    public static TextEmbeddingResponseParser createRandom() {
        return new TextEmbeddingResponseParser("$." + randomAlphaOfLength(5));
    }

    public void testFromMap() {
        var validation = new ValidationException();
        var parser = TextEmbeddingResponseParser.fromMap(
            new HashMap<>(Map.of(TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result[*].embeddings")),
            validation
        );

        assertThat(parser, is(new TextEmbeddingResponseParser("$.result[*].embeddings")));
    }

    public void testFromMap_ThrowsException_WhenRequiredFieldIsNotPresent() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> TextEmbeddingResponseParser.fromMap(new HashMap<>(Map.of("some_field", "$.result[*].embeddings")), validation)
        );

        assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: [json_parser] does not contain " + "the required setting [text_embeddings];")
        );
    }

    public void testToXContent() throws IOException {
        var entity = new TextEmbeddingResponseParser("$.result.path");

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
                    "text_embeddings": "$.result.path"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testParse() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var parser = new TextEmbeddingResponseParser("$.data[*].embedding");
        TextEmbeddingFloatResults parsedResults = (TextEmbeddingFloatResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(new TextEmbeddingFloatResults(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }))))
        );
    }

    public void testParse_MultipleEmbeddings() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  },
                  {
                      "object": "embedding",
                      "index": 1,
                      "embedding": [
                          1,
                          -2
                      ]
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var parser = new TextEmbeddingResponseParser("$.data[*].embedding");
        TextEmbeddingFloatResults parsedResults = (TextEmbeddingFloatResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new TextEmbeddingFloatResults(
                    List.of(
                        new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }),
                        new TextEmbeddingFloatResults.Embedding(new float[] { 1F, -2F })
                    )
                )
            )
        );
    }

    public void testParse_ThrowsException_WhenExtractedField_IsNotAListOfFloats() {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          1,
                          -0.015288644
                      ]
                  },
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          true,
                          -0.015288644
                      ]
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var parser = new TextEmbeddingResponseParser("$.data[*].embedding");
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse text embedding entry [1], error: Failed to parse list entry [0], error:"
                    + " Unable to convert field [data.embedding] of type [Boolean] to Number"
            )
        );
    }

    public void testParse_ThrowsException_WhenExtractedField_IsNotAList() {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": 1
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var parser = new TextEmbeddingResponseParser("$.data[*].embedding");
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse text embedding entry [0], error: Extracted field [data.embedding] "
                    + "is an invalid type, expected a list but received [Integer]"
            )
        );
    }

    @Override
    protected TextEmbeddingResponseParser mutateInstanceForVersion(TextEmbeddingResponseParser instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<TextEmbeddingResponseParser> instanceReader() {
        return TextEmbeddingResponseParser::new;
    }

    @Override
    protected TextEmbeddingResponseParser createTestInstance() {
        return createRandom();
    }

    @Override
    protected TextEmbeddingResponseParser mutateInstance(TextEmbeddingResponseParser instance) throws IOException {
        return randomValueOtherThan(instance, TextEmbeddingResponseParserTests::createRandom);
    }
}
