/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.cohere;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CohereEmbeddingsResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": [
                    [
                        -0.0018434525,
                        0.01777649
                    ]
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        InferenceServiceResults parsedResults = CohereEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(parsedResults, instanceOf(TextEmbeddingResults.class));
        MatcherAssert.assertThat(
            ((TextEmbeddingResults) parsedResults).embeddings(),
            is(List.of(new TextEmbeddingResults.Embedding(List.of(-0.0018434525F, 0.01777649F))))
        );
    }

    public void testFromResponse_CreatesResultsForASingleItem_ObjectFormat() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "float": [
                        [
                            -0.0018434525,
                            0.01777649
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        TextEmbeddingResults parsedResults = (TextEmbeddingResults) CohereEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(
            parsedResults.embeddings(),
            is(List.of(new TextEmbeddingResults.Embedding(List.of(-0.0018434525F, 0.01777649F))))
        );
    }

    public void testFromResponse_UsesTheFirstValidEmbeddingsEntry() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "float": [
                        [
                            -0.0018434525,
                            0.01777649
                        ]
                    ],
                    "int8": [
                        [
                            -1,
                            0
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        TextEmbeddingResults parsedResults = (TextEmbeddingResults) CohereEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(
            parsedResults.embeddings(),
            is(List.of(new TextEmbeddingResults.Embedding(List.of(-0.0018434525F, 0.01777649F))))
        );
    }

    public void testFromResponse_UsesTheFirstValidEmbeddingsEntryInt8_WithInvalidFirst() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "invalid_type": [
                        [
                            -0.0018434525,
                            0.01777649
                        ]
                    ],
                    "int8": [
                        [
                            -1,
                            0
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        TextEmbeddingByteResults parsedResults = (TextEmbeddingByteResults) CohereEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(
            parsedResults.embeddings(),
            is(List.of(new TextEmbeddingByteResults.Embedding(List.of((byte) -1, (byte) 0))))
        );
    }

    public void testFromResponse_ParsesBytes() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "int8": [
                        [
                            -1,
                            0
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        TextEmbeddingByteResults parsedResults = (TextEmbeddingByteResults) CohereEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(
            parsedResults.embeddings(),
            is(List.of(new TextEmbeddingByteResults.Embedding(List.of((byte) -1, (byte) 0))))
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": [
                    [
                        -0.0018434525,
                        0.01777649
                    ],
                    [
                        -0.123,
                        0.123
                    ]
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        TextEmbeddingResults parsedResults = (TextEmbeddingResults) CohereEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new TextEmbeddingResults.Embedding(List.of(-0.0018434525F, 0.01777649F)),
                    new TextEmbeddingResults.Embedding(List.of(-0.123F, 0.123F))
                )
            )
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems_ObjectFormat() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "float": [
                        [
                            -0.0018434525,
                            0.01777649
                        ],
                        [
                            -0.123,
                            0.123
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        TextEmbeddingResults parsedResults = (TextEmbeddingResults) CohereEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new TextEmbeddingResults.Embedding(List.of(-0.0018434525F, 0.01777649F)),
                    new TextEmbeddingResults.Embedding(List.of(-0.123F, 0.123F))
                )
            )
        );
    }

    public void testFromResponse_FailsWhenEmbeddingsFieldIsNotPresent() {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings_not_here": [
                    [
                        -0.0018434525,
                        0.01777649
                    ]
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> CohereEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Failed to find required field [embeddings] in Cohere embeddings response")
        );
    }

    public void testFromResponse_FailsWhenEmbeddingsByteValue_IsOutsideByteRange_Negative() {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "int8": [
                        [
                            -129,
                            127
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> CohereEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        MatcherAssert.assertThat(thrownException.getMessage(), is("Value [-129] is out of range for a byte"));
    }

    public void testFromResponse_FailsWhenEmbeddingsByteValue_IsOutsideByteRange_Positive() {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "int8": [
                        [
                            -128,
                            128
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> CohereEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        MatcherAssert.assertThat(thrownException.getMessage(), is("Value [128] is out of range for a byte"));
    }

    public void testFromResponse_FailsToFindAValidEmbeddingType() {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "invalid_type": [
                        [
                            -0.0018434525,
                            0.01777649
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> CohereEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Failed to find a supported embedding type in the Cohere embeddings response. Supported types are [float, int8]")
        );
    }
}
