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
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_TOKEN_PATH;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_WEIGHT_PATH;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SparseEmbeddingResponseParserTests extends AbstractBWCWireSerializationTestCase<SparseEmbeddingResponseParser> {

    public static SparseEmbeddingResponseParser createRandom() {
        return new SparseEmbeddingResponseParser(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testFromMap() {
        var validation = new ValidationException();
        var parser = SparseEmbeddingResponseParser.fromMap(
            new HashMap<>(
                Map.of(
                    SPARSE_EMBEDDING_TOKEN_PATH,
                    "$.result[*].embeddings[*].token",
                    SPARSE_EMBEDDING_WEIGHT_PATH,
                    "$.result[*].embeddings[*].weight"
                )
            ),
            validation
        );

        assertThat(parser, is(new SparseEmbeddingResponseParser("$.result[*].embeddings[*].token", "$.result[*].embeddings[*].weight")));
    }

    public void testFromMap_ThrowsException_WhenRequiredFieldsAreNotPresent() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> SparseEmbeddingResponseParser.fromMap(new HashMap<>(Map.of("not_path", "$.result[*].embeddings")), validation)
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: [json_parser] does not contain the required setting [token_path];"
                    + "2: [json_parser] does not contain the required setting [weight_path];"
            )
        );
    }

    public void testToXContent() throws IOException {
        var entity = new SparseEmbeddingResponseParser("$.result.path.token", "$.result.path.weight");

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
                    "token_path": "$.result.path.token",
                    "weight_path": "$.result.path.weight"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testParse() throws IOException {
        String responseJson = """
            {
                "request_id": "75C50B5B-E79E-4930-****-F48DBB392231",
                "latency": 22,
                "usage": {
                    "token_count": 11
                },
                "result": {
                    "sparse_embeddings": [
                        {
                            "index": 0,
                            "embedding": [
                                {
                                    "tokenId": 6,
                                    "weight": 0.101
                                },
                                {
                                    "tokenId": 163040,
                                    "weight": 0.28417
                                }
                            ]
                        }
                    ]
                }
            }
            """;

        var parser = new SparseEmbeddingResponseParser(
            "$.result.sparse_embeddings[*].embedding[*].tokenId",
            "$.result.sparse_embeddings[*].embedding[*].weight"
        );
        SparseEmbeddingResults parsedResults = (SparseEmbeddingResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new SparseEmbeddingResults(
                    List.of(
                        new SparseEmbeddingResults.Embedding(
                            List.of(new WeightedToken("6", 0.101f), new WeightedToken("163040", 0.28417f)),
                            false
                        )
                    )
                )
            )
        );
    }

    public void testParse_ThrowsException_WhenTheTokenField_IsNotAnArray() {
        String responseJson = """
            {
                "request_id": "75C50B5B-E79E-4930-****-F48DBB392231",
                "latency": 22,
                "usage": {
                    "token_count": 11
                },
                "result": {
                    "sparse_embeddings": [
                        {
                            "index": 0,
                            "tokenId": 6,
                            "weight": [0.101]
                        }
                    ]
                }
            }
            """;

        var parser = new SparseEmbeddingResponseParser("$.result.sparse_embeddings[*].tokenId", "$.result.sparse_embeddings[*].weight");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse sparse embedding entry [0], error: Extracted field [result.sparse_embeddings.tokenId] "
                    + "is an invalid type, expected a list but received [Integer]"
            )
        );
    }

    public void testParse_ThrowsException_WhenTheTokenArraySize_AndWeightArraySize_AreDifferent() {
        String responseJson = """
            {
                "request_id": "75C50B5B-E79E-4930-****-F48DBB392231",
                "latency": 22,
                "usage": {
                    "token_count": 11
                },
                "result": {
                    "sparse_embeddings": [
                        {
                            "index": 0,
                            "tokenId": [6, 7],
                            "weight": [0.101]
                        }
                    ]
                }
            }
            """;

        var parser = new SparseEmbeddingResponseParser("$.result.sparse_embeddings[*].tokenId", "$.result.sparse_embeddings[*].weight");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse sparse embedding entry [0], error: The extracted tokens list is size [2] "
                    + "but the weights list is size [1]. The list sizes must be equal."
            )
        );
    }

    public void testParse_ThrowsException_WhenTheWeightValue_IsNotAFloat() {
        String responseJson = """
            {
                "request_id": "75C50B5B-E79E-4930-****-F48DBB392231",
                "latency": 22,
                "usage": {
                    "token_count": 11
                },
                "result": {
                    "sparse_embeddings": [
                        {
                            "index": 0,
                            "tokenId": [6],
                            "weight": [true]
                        }
                    ]
                }
            }
            """;

        var parser = new SparseEmbeddingResponseParser("$.result.sparse_embeddings[*].tokenId", "$.result.sparse_embeddings[*].weight");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse sparse embedding entry [0], error: Failed to parse weight item: "
                    + "[0] of array, error: Unable to convert field [result.sparse_embeddings.weight] of type [Boolean] to Number"
            )
        );
    }

    public void testParse_ThrowsException_WhenTheWeightField_IsNotAnArray() {
        String responseJson = """
            {
                "request_id": "75C50B5B-E79E-4930-****-F48DBB392231",
                "latency": 22,
                "usage": {
                    "token_count": 11
                },
                "result": {
                    "sparse_embeddings": [
                        {
                            "index": 0,
                            "tokenId": [6],
                            "weight": 0.101
                        }
                    ]
                }
            }
            """;

        var parser = new SparseEmbeddingResponseParser("$.result.sparse_embeddings[*].tokenId", "$.result.sparse_embeddings[*].weight");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse sparse embedding entry [0], error: Extracted field [result.sparse_embeddings.weight] "
                    + "is an invalid type, expected a list but received [Double]"
            )
        );
    }

    public void testParse_ThrowsException_WhenExtractedField_IsNotFormattedCorrectly() {
        String responseJson = """
            {
                "request_id": "75C50B5B-E79E-4930-****-F48DBB392231",
                "latency": 22,
                "usage": {
                    "token_count": 11
                },
                "result": {
                    "sparse_embeddings": [
                        {
                            "index": 0,
                            "embedding": [
                                {
                                    "6": 0.101
                                },
                                {
                                    "163040": 0.28417
                                }
                            ]
                        }
                    ]
                }
            }
            """;

        var parser = new SparseEmbeddingResponseParser(
            "$.result.sparse_embeddings[*].embedding[*].tokenId",
            "$.result.sparse_embeddings[*].embedding[*].weight"
        );
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(exception.getMessage(), is("Unable to find field [tokenId] in map"));
    }

    @Override
    protected SparseEmbeddingResponseParser mutateInstanceForVersion(SparseEmbeddingResponseParser instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<SparseEmbeddingResponseParser> instanceReader() {
        return SparseEmbeddingResponseParser::new;
    }

    @Override
    protected SparseEmbeddingResponseParser createTestInstance() {
        return createRandom();
    }

    @Override
    protected SparseEmbeddingResponseParser mutateInstance(SparseEmbeddingResponseParser instance) throws IOException {
        return randomValueOtherThan(instance, SparseEmbeddingResponseParserTests::createRandom);
    }
}
