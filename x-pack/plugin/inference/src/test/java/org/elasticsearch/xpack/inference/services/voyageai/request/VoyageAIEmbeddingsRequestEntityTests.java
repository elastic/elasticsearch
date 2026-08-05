/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class VoyageAIEmbeddingsRequestEntityTests extends ESTestCase {

    private static final String TEST_INPUT = "some input";
    private static final String TEST_MODEL_ID = "some-model-id";
    private static final int TEST_RATE_LIMIT = 10;
    private static final int TEST_DIMENSIONS = 2048;
    private static final int TEST_MAX_INPUT_TOKENS = 1024;

    public void testXContent_WritesAllFields_Float() throws IOException {
        var embeddingType = VoyageAIEmbeddingType.FLOAT;
        assertToXContent_WritesAllFields(
            InputType.INTERNAL_SEARCH,
            new VoyageAIEmbeddingsServiceSettings(
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                embeddingType,
                SimilarityMeasure.DOT_PRODUCT,
                VoyageAIEmbeddingsRequestEntityTests.TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                false
            ),
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            Strings.format("""
                {
                    "input": ["%s"],
                    "model": "%s",
                    "input_type": "query",
                    "output_dimension": %d,
                    "output_dtype": "%s"
                }
                """, TEST_INPUT, TEST_MODEL_ID, TEST_DIMENSIONS, embeddingType.toString())
        );
    }

    public void testXContent_WritesAllFields_Int8() throws IOException {
        var embeddingType = VoyageAIEmbeddingType.INT8;
        assertToXContent_WritesAllFields(
            InputType.INGEST,
            new VoyageAIEmbeddingsServiceSettings(
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                embeddingType,
                SimilarityMeasure.DOT_PRODUCT,
                VoyageAIEmbeddingsRequestEntityTests.TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                false
            ),
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            Strings.format("""
                {
                    "input": ["%s"],
                    "model": "%s",
                    "input_type": "document",
                    "output_dimension": %d,
                    "output_dtype": "%s"
                }
                """, TEST_INPUT, TEST_MODEL_ID, TEST_DIMENSIONS, embeddingType.toString())
        );
    }

    public void testXContent_WritesAllFields_Binary() throws IOException {
        var embeddingType = VoyageAIEmbeddingType.BINARY;
        assertToXContent_WritesAllFields(
            InputType.INTERNAL_SEARCH,
            new VoyageAIEmbeddingsServiceSettings(
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                embeddingType,
                SimilarityMeasure.DOT_PRODUCT,
                VoyageAIEmbeddingsRequestEntityTests.TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                false
            ),
            new VoyageAIEmbeddingsTaskSettings(null, null),
            Strings.format("""
                {
                    "input": ["%s"],
                    "model": "%s",
                    "input_type": "query",
                    "output_dimension": %d,
                    "output_dtype": "%s"
                }
                """, TEST_INPUT, TEST_MODEL_ID, TEST_DIMENSIONS, embeddingType.toString())
        );
    }

    public void testXContent_FallsBackToTaskSettingsInputType_WhenRootInputTypeIsNull() throws IOException {
        var embeddingType = VoyageAIEmbeddingType.FLOAT;
        assertToXContent_WritesAllFields(
            null,
            new VoyageAIEmbeddingsServiceSettings(
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                embeddingType,
                SimilarityMeasure.DOT_PRODUCT,
                VoyageAIEmbeddingsRequestEntityTests.TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                false
            ),
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            Strings.format("""
                {
                    "input": ["%s"],
                    "model": "%s",
                    "input_type": "document",
                    "output_dimension": %d,
                    "output_dtype": "%s"
                }
                """, TEST_INPUT, TEST_MODEL_ID, TEST_DIMENSIONS, embeddingType.toString())
        );
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var embeddingType = VoyageAIEmbeddingType.FLOAT;
        assertToXContent_WritesAllFields(
            null,
            new VoyageAIEmbeddingsServiceSettings(TEST_MODEL_ID, null, embeddingType, null, null, null, false),
            VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            Strings.format("""
                {
                    "input": ["%s"],
                    "model": "%s",
                    "output_dtype": "%s"
                }
                """, TEST_INPUT, TEST_MODEL_ID, embeddingType.toString())
        );
    }

    private static void assertToXContent_WritesAllFields(
        InputType inputType,
        VoyageAIEmbeddingsServiceSettings serviceSettings,
        VoyageAIEmbeddingsTaskSettings taskSettings,
        String expectedResult
    ) throws IOException {
        var entity = new VoyageAIEmbeddingsRequestEntity(List.of(TEST_INPUT), inputType, serviceSettings, taskSettings);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(expectedResult)));
    }
}
