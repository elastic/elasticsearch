/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.response;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public abstract class CohereResponseTests extends ESTestCase {

    protected abstract InferenceServiceResults parseResponse(String responseJson) throws IOException;

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

        var parsedResults = parseResponse(responseJson);
        assertFloatEmbeddings(parsedResults, new float[] { -0.0018434525F, 0.01777649F });
    }

    protected void assertFloatEmbeddings(InferenceServiceResults results, float[] expectedResponse) {
        MatcherAssert.assertThat(results, instanceOf(TextEmbeddingFloatResults.class));
        MatcherAssert.assertThat(
            ((TextEmbeddingFloatResults) results).embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(expectedResponse)))
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

        var parsedResults = parseResponse(responseJson);
        assertFloatEmbeddings(parsedResults, new float[] { -0.0018434525F, 0.01777649F });
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

        var parsedResults = parseResponse(responseJson);
        assertFloatEmbeddings(parsedResults, new float[] { -0.0018434525F, 0.01777649F });
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

        var parsedResults = parseResponse(responseJson);
        assertByteEmbeddings(parsedResults, new byte[] { (byte) -1, (byte) 0 });
    }

    private void assertByteEmbeddings(InferenceServiceResults results, byte[] expectedResponse) {
        MatcherAssert.assertThat(results, instanceOf(TextEmbeddingByteResults.class));
        MatcherAssert.assertThat(
            ((TextEmbeddingByteResults) results).embeddings(),
            is(List.of(new TextEmbeddingByteResults.Embedding(expectedResponse)))
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

        var parsedResults = parseResponse(responseJson);
        assertByteEmbeddings(parsedResults, new byte[] { (byte) -1, (byte) 0 });
    }

    public void testFromResponse_ParsesBytes_FromBinaryEmbeddingsEntry() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "binary": [
                        [
                            -55,
                            74,
                            101,
                            67,
                            83
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "2"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_by_type"
            }
            """;

        var parsedResults = parseResponse(responseJson);
        assertBitEmbeddings(parsedResults, new byte[] { (byte) -55, (byte) 74, (byte) 101, (byte) 67, (byte) 83 });
    }

    private void assertBitEmbeddings(InferenceServiceResults results, byte[] expectedResponse) {
        MatcherAssert.assertThat(results, instanceOf(TextEmbeddingBitResults.class));
        MatcherAssert.assertThat(
            ((TextEmbeddingBitResults) results).embeddings(),
            is(List.of(new TextEmbeddingByteResults.Embedding(expectedResponse)))
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

        var parsedResults = parseResponse(responseJson);
        assertFloatEmbeddings(parsedResults, List.of(new float[] { -0.0018434525F, 0.01777649F }, new float[] { -0.123F, 0.123F }));
    }

    private void assertFloatEmbeddings(InferenceServiceResults results, List<float[]> expectedResponse) {
        MatcherAssert.assertThat(results, instanceOf(TextEmbeddingFloatResults.class));

        var expectedList = expectedResponse.stream().map(TextEmbeddingFloatResults.Embedding::new).toList();
        MatcherAssert.assertThat(((TextEmbeddingFloatResults) results).embeddings(), is(expectedList));
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

        var parsedResults = parseResponse(responseJson);
        assertFloatEmbeddings(parsedResults, List.of(new float[] { -0.0018434525F, 0.01777649F }, new float[] { -0.123F, 0.123F }));
    }

    public void testFromResponse_CreatesResultsForMultipleItems_ObjectFormat_Binary() throws IOException {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello",
                    "goodbye"
                ],
                "embeddings": {
                    "binary": [
                        [
                            -55,
                            74,
                            101,
                            67
                        ],
                        [
                            34,
                            -64,
                            97,
                            65,
                            -42
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "2"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_by_type"
            }
            """;

        var parsedResults = parseResponse(responseJson);
        assertBitEmbeddings(
            parsedResults,
            List.of(
                new byte[] { (byte) -55, (byte) 74, (byte) 101, (byte) 67 },
                new byte[] { (byte) 34, (byte) -64, (byte) 97, (byte) 65, (byte) -42 }
            )
        );
    }

    private void assertBitEmbeddings(InferenceServiceResults results, List<byte[]> expectedResponse) {
        MatcherAssert.assertThat(results, instanceOf(TextEmbeddingBitResults.class));

        var expectedList = expectedResponse.stream().map(TextEmbeddingByteResults.Embedding::new).toList();
        MatcherAssert.assertThat(((TextEmbeddingBitResults) results).embeddings(), is(expectedList));
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

        var thrownException = expectThrows(IllegalStateException.class, () -> parseResponse(responseJson));

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

        var thrownException = expectThrows(IllegalArgumentException.class, () -> parseResponse(responseJson));

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

        var thrownException = expectThrows(IllegalArgumentException.class, () -> parseResponse(responseJson));

        MatcherAssert.assertThat(thrownException.getMessage(), is("Value [128] is out of range for a byte"));
    }

    public void testFromResponse_FailsWhenEmbeddingsBinaryValue_IsOutsideByteRange_Negative() {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "binary": [
                        [
                            -129,
                            127
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "2"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_by_type"
            }
            """;

        var thrownException = expectThrows(IllegalArgumentException.class, () -> parseResponse(responseJson));

        MatcherAssert.assertThat(thrownException.getMessage(), is("Value [-129] is out of range for a byte"));
    }

    public void testFromResponse_FailsWhenEmbeddingsBinaryValue_IsOutsideByteRange_Positive() {
        String responseJson = """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": {
                    "binary": [
                        [
                            -128,
                            128
                        ]
                    ]
                },
                "meta": {
                    "api_version": {
                        "version": "2"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_by_type"
            }
            """;

        var thrownException = expectThrows(IllegalArgumentException.class, () -> parseResponse(responseJson));

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

        var thrownException = expectThrows(IllegalStateException.class, () -> parseResponse(responseJson));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Failed to find a supported embedding type in the Cohere embeddings response. Supported types are [binary, float, int8]")
        );
    }
}
