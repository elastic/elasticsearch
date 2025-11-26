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
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceEmbeddingType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.custom.response.DenseEmbeddingResponseParser.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.custom.response.DenseEmbeddingResponseParser.TEXT_EMBEDDING_PARSER_EMBEDDINGS;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class DenseEmbeddingResponseParserTests extends AbstractBWCWireSerializationTestCase<DenseEmbeddingResponseParser> {

    private static final TransportVersion ML_INFERENCE_CUSTOM_SERVICE_EMBEDDING_TYPE = TransportVersion.fromName(
        "ml_inference_custom_service_embedding_type"
    );

    public static DenseEmbeddingResponseParser createRandom() {
        return new DenseEmbeddingResponseParser("$." + randomAlphaOfLength(5), randomFrom(CustomServiceEmbeddingType.values()));
    }

    public void testFromMap() {
        var validation = new ValidationException();
        var parser = DenseEmbeddingResponseParser.fromMap(
            new HashMap<>(
                Map.of(
                    TEXT_EMBEDDING_PARSER_EMBEDDINGS,
                    "$.result[*].embeddings",
                    EMBEDDING_TYPE,
                    CustomServiceEmbeddingType.BIT.toString()
                )
            ),
            "scope",
            validation
        );

        assertThat(parser, is(new DenseEmbeddingResponseParser("$.result[*].embeddings", CustomServiceEmbeddingType.BIT)));
    }

    public void testFromMap_ThrowsException_WhenRequiredFieldIsNotPresent() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> DenseEmbeddingResponseParser.fromMap(new HashMap<>(Map.of("some_field", "$.result[*].embeddings")), "scope", validation)
        );

        assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: [scope.json_parser] does not contain the required setting [text_embeddings];")
        );
    }

    public void testToXContent() throws IOException {
        var entity = new DenseEmbeddingResponseParser("$.result.path", CustomServiceEmbeddingType.BINARY);

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
                    "text_embeddings": "$.result.path",
                    "embedding_type": "binary"
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

        var parser = new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT);
        DenseEmbeddingFloatResults parsedResults = (DenseEmbeddingFloatResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new DenseEmbeddingFloatResults(
                    List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }))
                )
            )
        );
    }

    public void testParseByte() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
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

        var parser = new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.BYTE);
        DenseEmbeddingByteResults parsedResults = (DenseEmbeddingByteResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(new DenseEmbeddingByteResults(List.of(new DenseEmbeddingByteResults.Embedding(new byte[] { 1, -2 }))))
        );
    }

    public void testParseBit() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
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

        var parser = new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.BIT);
        DenseEmbeddingBitResults parsedResults = (DenseEmbeddingBitResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, is(new DenseEmbeddingBitResults(List.of(new DenseEmbeddingByteResults.Embedding(new byte[] { 1, -2 })))));
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

        var parser = new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT);
        DenseEmbeddingFloatResults parsedResults = (DenseEmbeddingFloatResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new DenseEmbeddingFloatResults(
                    List.of(
                        new DenseEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }),
                        new DenseEmbeddingFloatResults.Embedding(new float[] { 1F, -2F })
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

        var parser = new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT);
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

        var parser = new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT);
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
    protected DenseEmbeddingResponseParser mutateInstanceForVersion(DenseEmbeddingResponseParser instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_CUSTOM_SERVICE_EMBEDDING_TYPE) == false) {
            return new DenseEmbeddingResponseParser(instance.getTextEmbeddingsPath(), CustomServiceEmbeddingType.FLOAT);
        }
        return instance;
    }

    @Override
    protected Writeable.Reader<DenseEmbeddingResponseParser> instanceReader() {
        return DenseEmbeddingResponseParser::new;
    }

    @Override
    protected DenseEmbeddingResponseParser createTestInstance() {
        return createRandom();
    }

    @Override
    protected DenseEmbeddingResponseParser mutateInstance(DenseEmbeddingResponseParser instance) throws IOException {
        if (randomBoolean()) {
            var textEmbeddingPath = randomValueOtherThan(instance.getTextEmbeddingsPath(), () -> "$." + randomAlphaOfLength(5));
            return new DenseEmbeddingResponseParser(textEmbeddingPath, instance.getEmbeddingType());
        } else {
            var embeddingType = randomValueOtherThan(instance.getEmbeddingType(), () -> randomFrom(CustomServiceEmbeddingType.values()));
            return new DenseEmbeddingResponseParser(instance.getTextEmbeddingsPath(), embeddingType);
        }
    }
}
