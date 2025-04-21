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

import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_TOKEN_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_WEIGHT_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_RESULT_PATH;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SparseEmbeddingResponseParserTests extends AbstractBWCWireSerializationTestCase<SparseEmbeddingResponseParser> {

    public static SparseEmbeddingResponseParser createRandom() {
        return new SparseEmbeddingResponseParser("$." + randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testFromMap() {
        var validation = new ValidationException();
        var parser = SparseEmbeddingResponseParser.fromMap(
            new HashMap<>(
                Map.of(
                    SPARSE_RESULT_PATH,
                    "$.result[*].embeddings",
                    SPARSE_EMBEDDING_TOKEN_FIELD_NAME,
                    "token_id",
                    SPARSE_EMBEDDING_WEIGHT_FIELD_NAME,
                    "weight_id"
                )
            ),
            validation
        );

        assertThat(parser, is(new SparseEmbeddingResponseParser("$.result[*].embeddings", "token_id", "weight_id")));
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
                "Validation Failed: 1: [json_parser] does not contain the required setting [path];"
                    + "2: [json_parser] does not contain the required setting [token_field_name];"
                    + "3: [json_parser] does not contain the required setting [weight_field_name];"
            )
        );
    }

    public void testToXContent() throws IOException {
        var entity = new SparseEmbeddingResponseParser("$.result.path", "token_id", "weight_id");

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
                    "path": "$.result.path",
                    "token_field_name": "token_id",
                    "weight_field_name": "weight_id"
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

        var parser = new SparseEmbeddingResponseParser("$.result.sparse_embeddings[*].embedding[*]", "tokenId", "weight");
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

        var parser = new SparseEmbeddingResponseParser("$.result.sparse_embeddings[*].embedding", "tokenId", "weight");
        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(exception.getMessage(), is("Failed to find token id field: [tokenId]"));
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
