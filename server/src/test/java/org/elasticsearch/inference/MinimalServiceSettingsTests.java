/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class MinimalServiceSettingsTests extends AbstractBWCSerializationTestCase<MinimalServiceSettings> {

    private static final int TEST_DIMENSIONS_384 = 384;
    private static final int TEST_DIMENSIONS_768 = 768;
    private static final String SERVICE_A = "service-a";
    private static final String SERVICE_B = "service-b";
    private static final String SERVICE = "service";
    private static final String OTHER_SERVICE = "other-service";

    private static final MinimalServiceSettings MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA = new MinimalServiceSettings(
        SERVICE_A,
        TaskType.TEXT_EMBEDDING,
        TEST_DIMENSIONS_384,
        SimilarityMeasure.COSINE,
        DenseVectorFieldMapper.ElementType.FLOAT,
        EndpointMetadata.EMPTY_INSTANCE
    );

    private static final String MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA_JSON = """
        {
          "service": "service-a",
          "task_type": "text_embedding",
          "dimensions": 384,
          "similarity": "cosine",
          "element_type": "float"
        }
        """;

    private static final MinimalServiceSettings MINIMAL_SERVICE_SETTINGS_WITH_METADATA = new MinimalServiceSettings(
        SERVICE_A,
        TaskType.TEXT_EMBEDDING,
        384,
        SimilarityMeasure.COSINE,
        DenseVectorFieldMapper.ElementType.FLOAT,
        new EndpointMetadata(
            new EndpointMetadata.Heuristics(List.of("heuristic1", "heuristic2"), StatusHeuristic.BETA, "2025-01-01", "2025-12-31"),
            new EndpointMetadata.Internal("fingerprint", 1L),
            new EndpointMetadata.Display("name")
        )
    );

    private static final String MINIMAL_SERVICE_SETTINGS_WITH_METADATA_JSON = """
        {
          "service": "service-a",
          "task_type": "text_embedding",
          "dimensions": 384,
          "similarity": "cosine",
          "element_type": "float",
          "metadata": {
            "heuristics": {
              "properties": ["heuristic1", "heuristic2"],
              "status": "beta",
              "release_date": "2025-01-01",
              "end_of_life_date": "2025-12-31"
            },
            "internal": {
              "fingerprint": "fingerprint",
              "version": 1
            },
            "display": {
              "name": "name"
            }
          }
        }
        """;

    public static MinimalServiceSettings randomInstance() {
        TaskType taskType = randomFrom(TaskType.values());
        Integer dimensions = null;
        SimilarityMeasure similarity = null;
        DenseVectorFieldMapper.ElementType elementType = null;

        if (taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.EMBEDDING) {
            dimensions = randomIntBetween(2, 1024);
            similarity = randomFrom(SimilarityMeasure.values());
            elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
        }
        var endpointMetadata = randomBoolean() ? EndpointMetadata.EMPTY_INSTANCE : EndpointMetadataTests.randomInstance();
        return new MinimalServiceSettings(
            randomBoolean() ? null : randomAlphaOfLength(10),
            taskType,
            dimensions,
            similarity,
            elementType,
            endpointMetadata
        );
    }

    @Override
    protected Writeable.Reader<MinimalServiceSettings> instanceReader() {
        return MinimalServiceSettings::new;
    }

    @Override
    protected MinimalServiceSettings createTestInstance() {
        return randomInstance();
    }

    @Override
    protected MinimalServiceSettings mutateInstance(MinimalServiceSettings instance) throws IOException {
        var service = instance.service();
        var taskType = instance.taskType();
        var dimensions = instance.dimensions();
        var similarity = instance.similarity();
        var elementType = instance.elementType();
        var endpointMetadata = instance.endpointMetadata();

        boolean instanceHasEmbeddingTaskType = taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.EMBEDDING;

        switch (randomIntBetween(0, 5)) {
            case 0 -> service = randomValueOtherThan(service, () -> randomAlphaOfLengthOrNull(10));
            case 1 -> {
                taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
                // Update dimensions, similarity, elementType based on new taskType
                if ((taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.EMBEDDING)) {
                    if (instanceHasEmbeddingTaskType == false) {
                        dimensions = randomIntBetween(2, 1024);
                        similarity = randomFrom(SimilarityMeasure.values());
                        elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
                    }
                } else {
                    dimensions = null;
                    similarity = null;
                    elementType = null;
                }
            }
            case 2 -> {
                if (instanceHasEmbeddingTaskType) {
                    dimensions = randomValueOtherThan(dimensions, () -> randomIntBetween(2, 1024));
                } else {
                    // Change taskType to TEXT_EMBEDDING to make dimensions applicable
                    taskType = TaskType.TEXT_EMBEDDING;
                    dimensions = randomIntBetween(2, 1024);
                    similarity = randomFrom(SimilarityMeasure.values());
                    elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
                }
            }
            case 3 -> {
                if (instanceHasEmbeddingTaskType) {
                    similarity = randomValueOtherThan(similarity, () -> randomFrom(SimilarityMeasure.values()));
                } else {
                    // Change taskType to TEXT_EMBEDDING to make similarity applicable
                    taskType = TaskType.TEXT_EMBEDDING;
                    dimensions = randomIntBetween(2, 1024);
                    similarity = randomFrom(SimilarityMeasure.values());
                    elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
                }
            }
            case 4 -> {
                if (instanceHasEmbeddingTaskType) {
                    elementType = randomValueOtherThan(elementType, () -> randomFrom(DenseVectorFieldMapper.ElementType.values()));
                } else {
                    // Change taskType to TEXT_EMBEDDING to make elementType applicable
                    taskType = TaskType.TEXT_EMBEDDING;
                    dimensions = randomIntBetween(2, 1024);
                    similarity = randomFrom(SimilarityMeasure.values());
                    elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
                }
            }
            case 5 -> {
                // Ensure we always get a different value: if EMPTY, use non-EMPTY; if non-EMPTY, use EMPTY or different instance
                if (endpointMetadata.equals(EndpointMetadata.EMPTY_INSTANCE)) {
                    endpointMetadata = EndpointMetadataTests.randomNonEmptyInstance();
                } else {
                    endpointMetadata = randomValueOtherThan(endpointMetadata, EndpointMetadataTests::randomInstance);
                }
            }
        }

        return new MinimalServiceSettings(service, taskType, dimensions, similarity, elementType, endpointMetadata);
    }

    @Override
    protected MinimalServiceSettings mutateInstanceForVersion(MinimalServiceSettings instance, TransportVersion version) {
        var metadataVersion = TransportVersion.fromName("inference_endpoint_metadata_fields_added");

        if (version.supports(metadataVersion)) {
            return instance;
        } else {
            return new MinimalServiceSettings(
                instance.service(),
                instance.taskType(),
                instance.dimensions(),
                instance.similarity(),
                instance.elementType(),
                EndpointMetadata.EMPTY_INSTANCE
            );
        }
    }

    @Override
    protected MinimalServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return MinimalServiceSettings.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testCanMergeWith_SettingsWithDifferentEndpointMetadata() {
        var settings = new MinimalServiceSettings(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        var same = new MinimalServiceSettings(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataTests.randomNonEmptyInstance()
        );
        assertTrue(settings.canMergeWith(same));
    }

    public void testCanMergeWithSameSettings() {
        var settings = new MinimalServiceSettings(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        var same = new MinimalServiceSettings(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        assertTrue(settings.canMergeWith(same));
    }

    public void testCanMergeWithDifferentServiceName_ReturnsTrue() {
        // Embedding task type
        {
            var settings = new MinimalServiceSettings(
                SERVICE_A,
                TaskType.TEXT_EMBEDDING,
                TEST_DIMENSIONS_384,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT,
                EndpointMetadata.EMPTY_INSTANCE
            );
            var other = new MinimalServiceSettings(
                SERVICE_B,
                TaskType.TEXT_EMBEDDING,
                TEST_DIMENSIONS_384,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT,
                EndpointMetadata.EMPTY_INSTANCE
            );
            assertTrue(settings.canMergeWith(other));
        }
        // Non-embedding task type
        {
            var settings = new MinimalServiceSettings(SERVICE, TaskType.COMPLETION, null, null, null, EndpointMetadata.EMPTY_INSTANCE);
            var other = new MinimalServiceSettings(OTHER_SERVICE, TaskType.COMPLETION, null, null, null, EndpointMetadata.EMPTY_INSTANCE);
            assertTrue(settings.canMergeWith(other));
        }
    }

    public void testCanMergeWithDifferentTaskType_ReturnsFalse() {
        var settings = new MinimalServiceSettings(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        var other = new MinimalServiceSettings(null, TaskType.SPARSE_EMBEDDING, null, null, null, EndpointMetadata.EMPTY_INSTANCE);
        assertFalse(settings.canMergeWith(other));
    }

    public void testCanMergeWithDifferentDimensions_ReturnsFalse() {
        var settings = new MinimalServiceSettings(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        var other = new MinimalServiceSettings(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_768,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        assertFalse(settings.canMergeWith(other));
    }

    public void testCanMergeWithDifferentSimilarity_ReturnsFalse() {
        var settings = new MinimalServiceSettings(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        var other = new MinimalServiceSettings(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.DOT_PRODUCT,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        assertFalse(settings.canMergeWith(other));
    }

    public void testCanMergeWithDifferentElementType_ReturnsFalse() {
        var settings = new MinimalServiceSettings(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadata.EMPTY_INSTANCE
        );
        var other = new MinimalServiceSettings(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.BYTE,
            EndpointMetadata.EMPTY_INSTANCE
        );
        assertFalse(settings.canMergeWith(other));
    }

    public void testToXContentMinimalServiceSettingsWithoutMetadata() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA.toXContent(builder, ToXContent.EMPTY_PARAMS);
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace(MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA_JSON)));
    }

    public void testToXContentMinimalServiceSettingsWithEndpointMetadata() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        MINIMAL_SERVICE_SETTINGS_WITH_METADATA.toXContent(builder, ToXContent.EMPTY_PARAMS);
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace(MINIMAL_SERVICE_SETTINGS_WITH_METADATA_JSON)));
    }
}
