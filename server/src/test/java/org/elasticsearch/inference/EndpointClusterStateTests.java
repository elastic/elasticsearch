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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.inference.metadata.EndpointMetadataClusterState;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class EndpointClusterStateTests extends AbstractBWCSerializationTestCase<EndpointClusterState> {

    private static final int TEST_DIMENSIONS_384 = 384;
    private static final int TEST_DIMENSIONS_768 = 768;
    private static final String SERVICE_A = "service-a";
    private static final String SERVICE_B = "service-b";
    private static final String SERVICE = "service";
    private static final String OTHER_SERVICE = "other-service";

    private static final EndpointClusterState MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA = new EndpointClusterState(
        SERVICE_A,
        TaskType.TEXT_EMBEDDING,
        TEST_DIMENSIONS_384,
        SimilarityMeasure.COSINE,
        DenseVectorFieldMapper.ElementType.FLOAT,
        EndpointMetadataClusterState.EMPTY_INSTANCE
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

    private static final EndpointClusterState MINIMAL_SERVICE_SETTINGS_WITH_METADATA = new EndpointClusterState(
        SERVICE_A,
        TaskType.TEXT_EMBEDDING,
        384,
        SimilarityMeasure.COSINE,
        DenseVectorFieldMapper.ElementType.FLOAT,
        new EndpointMetadataClusterState(
            new EndpointMetadata.Heuristics(List.of("heuristic1", "heuristic2"), StatusHeuristic.BETA, "2025-01-01", "2025-12-31"),
            new EndpointMetadata.Internal("fingerprint", 1L)
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
            }
          }
        }
        """;

    public static EndpointClusterState randomInstance() {
        TaskType taskType = randomFrom(EnumSet.complementOf(EnumSet.of(TaskType.ANY)));
        Integer dimensions = null;
        SimilarityMeasure similarity = null;
        DenseVectorFieldMapper.ElementType elementType = null;

        if (taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.EMBEDDING) {
            dimensions = randomIntBetween(2, 1024);
            similarity = randomFrom(SimilarityMeasure.values());
            elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
        }
        var endpointMetadata = randomBoolean()
            ? EndpointMetadataClusterState.EMPTY_INSTANCE
            : EndpointMetadataClusterStateTests.randomInstance();
        return new EndpointClusterState(
            randomBoolean() ? null : randomAlphaOfLength(10),
            taskType,
            dimensions,
            similarity,
            elementType,
            endpointMetadata
        );
    }

    @Override
    protected Writeable.Reader<EndpointClusterState> instanceReader() {
        return EndpointClusterState::new;
    }

    @Override
    protected EndpointClusterState createTestInstance() {
        return randomInstance();
    }

    @Override
    protected EndpointClusterState mutateInstance(EndpointClusterState instance) throws IOException {
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
                taskType = randomValueOtherThan(taskType, () -> randomFrom(EnumSet.complementOf(EnumSet.of(TaskType.ANY))));
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
                if (endpointMetadata.equals(EndpointMetadataClusterState.EMPTY_INSTANCE)) {
                    endpointMetadata = EndpointMetadataClusterStateTests.randomNonEmptyInstance();
                } else {
                    endpointMetadata = randomValueOtherThan(endpointMetadata, EndpointMetadataClusterStateTests::randomInstance);
                }
            }
        }

        return new EndpointClusterState(service, taskType, dimensions, similarity, elementType, endpointMetadata);
    }

    @Override
    protected EndpointClusterState mutateInstanceForVersion(EndpointClusterState instance, TransportVersion version) {
        var endpointMetadata = instance.endpointMetadata();
        if (version.supports(EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED) == false) {
            endpointMetadata = EndpointMetadataClusterState.EMPTY_INSTANCE;
        }
        // EndpointMetadataClusterState has no version-gated fields of its own — heuristics and internal are always written in full.
        return new EndpointClusterState(
            instance.service(),
            instance.taskType(),
            instance.dimensions(),
            instance.similarity(),
            instance.elementType(),
            endpointMetadata
        );
    }

    @Override
    protected EndpointClusterState doParseInstance(XContentParser parser) throws IOException {
        return EndpointClusterState.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testCanMergeWith_SettingsWithDifferentEndpointMetadata() {
        var settings = new EndpointClusterState(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        var same = new EndpointClusterState(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterStateTests.randomNonEmptyInstance()
        );
        assertTrue(settings.canMergeWith(same));
    }

    public void testCanMergeWithSameSettings() {
        var settings = new EndpointClusterState(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        var same = new EndpointClusterState(
            SERVICE_A,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        assertTrue(settings.canMergeWith(same));
    }

    public void testCanMergeWithDifferentServiceName_ReturnsTrue() {
        // Embedding task type
        {
            var settings = new EndpointClusterState(
                SERVICE_A,
                TaskType.TEXT_EMBEDDING,
                TEST_DIMENSIONS_384,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT,
                EndpointMetadataClusterState.EMPTY_INSTANCE
            );
            var other = new EndpointClusterState(
                SERVICE_B,
                TaskType.TEXT_EMBEDDING,
                TEST_DIMENSIONS_384,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT,
                EndpointMetadataClusterState.EMPTY_INSTANCE
            );
            assertTrue(settings.canMergeWith(other));
        }
        // Non-embedding task type
        {
            var settings = new EndpointClusterState(
                SERVICE,
                TaskType.COMPLETION,
                null,
                null,
                null,
                EndpointMetadataClusterState.EMPTY_INSTANCE
            );
            var other = new EndpointClusterState(
                OTHER_SERVICE,
                TaskType.COMPLETION,
                null,
                null,
                null,
                EndpointMetadataClusterState.EMPTY_INSTANCE
            );
            assertTrue(settings.canMergeWith(other));
        }
    }

    public void testCanMergeWithDifferentTaskType_ReturnsFalse() {
        var settings = new EndpointClusterState(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        var other = new EndpointClusterState(
            null,
            TaskType.SPARSE_EMBEDDING,
            null,
            null,
            null,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        assertFalse(settings.canMergeWith(other));
    }

    public void testCanMergeWithDifferentDimensions_ReturnsFalse() {
        var settings = new EndpointClusterState(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        var other = new EndpointClusterState(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_768,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        assertFalse(settings.canMergeWith(other));
    }

    public void testCanMergeWithDifferentSimilarity_ReturnsFalse() {
        var settings = new EndpointClusterState(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        var other = new EndpointClusterState(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.DOT_PRODUCT,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        assertFalse(settings.canMergeWith(other));
    }

    public void testCanMergeWithDifferentElementType_ReturnsFalse() {
        var settings = new EndpointClusterState(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        var other = new EndpointClusterState(
            null,
            TaskType.TEXT_EMBEDDING,
            TEST_DIMENSIONS_384,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.BYTE,
            EndpointMetadataClusterState.EMPTY_INSTANCE
        );
        assertFalse(settings.canMergeWith(other));
    }

    public void testToXContent_WithEmptyEndpointMetadata_DoesNotSerializeEndpointMetadata() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA.toXContent(builder, ToXContent.EMPTY_PARAMS);
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace(MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA_JSON)));
    }

    public void testToXContent_WithEndpointMetadata_SerializesEndpointMetadata() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        MINIMAL_SERVICE_SETTINGS_WITH_METADATA.toXContent(builder, ToXContent.EMPTY_PARAMS);
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace(MINIMAL_SERVICE_SETTINGS_WITH_METADATA_JSON)));
    }

    public void testToXContent_DoesNotSerializeEndpointMetadata_WhenPassingParamWithoutEndpointMetadata() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        MINIMAL_SERVICE_SETTINGS_WITH_METADATA.toXContent(builder, EndpointClusterState.withoutEndpointMetadata());
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace(MINIMAL_SERVICE_SETTINGS_WITHOUT_METADATA_JSON)));
    }

    // BWC stream tests
    // Both read and write directions must fully consume the stream: a misaligned read at
    // ModelRegistryClusterStateMetadata.java:170's map reader would corrupt the entire cluster-state custom.

    private static final TaskType LEGACY_TEST_TASK_TYPE = TaskType.COMPLETION;

    private static void writeNonMetadataPrefix(StreamOutput out, String service) throws IOException {
        out.writeOptionalString(service);
        LEGACY_TEST_TASK_TYPE.writeTo(out);
        out.writeOptionalInt(null);
        out.writeOptionalEnum(null);
        out.writeOptionalEnum(null);
    }

    private static void readNonMetadataPrefix(StreamInput in) throws IOException {
        in.readOptionalString();
        TaskType.fromStream(in);
        in.readOptionalInt();
        in.readOptionalEnum(SimilarityMeasure.class);
        in.readOptionalEnum(DenseVectorFieldMapper.ElementType.class);
    }

    /**
     * Verifies that reading a stream written at the 9.4-era version (full EndpointMetadata without regions) produces the correct
     * EndpointClusterState subset and fully consumes the stream.
     */
    public void testReadFrom_LegacyStream_9_4Era_ConsumesFullLayout() throws IOException {
        var service = randomAlphaOfLength(10);
        var heuristics = EndpointMetadataTests.randomHeuristics();
        var internal = EndpointMetadataTests.randomInternal();
        var display = EndpointMetadataTests.randomDisplay();
        var original = new EndpointMetadata(EndpointMetadata.ModelIdentity.EMPTY_INSTANCE, heuristics, internal, display, List.of(), false);

        var out = new BytesStreamOutput();
        out.setTransportVersion(EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED);
        writeNonMetadataPrefix(out, service);
        original.writeTo(out);

        var in = out.bytes().streamInput();
        in.setTransportVersion(EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED);
        var parsed = new EndpointClusterState(in);

        assertThat(parsed.endpointMetadata().heuristics(), is(heuristics));
        assertThat(parsed.endpointMetadata().internal(), is(internal));
        assertThat(in.available(), is(0));
    }

    /**
     * Verifies that reading a stream written at the 9.5-era version (full EndpointMetadata with regions) produces the correct
     * EndpointClusterState subset and fully consumes the stream.
     */
    public void testReadFrom_LegacyStream_9_5Era_ConsumesFullLayout() throws IOException {
        var service = randomAlphaOfLength(10);
        var heuristics = EndpointMetadataTests.randomHeuristics();
        var internal = EndpointMetadataTests.randomInternal();
        var display = EndpointMetadataTests.randomDisplay();
        var regions = EndpointMetadataTests.randomRegions();
        var original = new EndpointMetadata(EndpointMetadata.ModelIdentity.EMPTY_INSTANCE, heuristics, internal, display, regions, true);

        var out = new BytesStreamOutput();
        out.setTransportVersion(EndpointMetadata.REGIONS_ADDED);
        writeNonMetadataPrefix(out, service);
        original.writeTo(out);

        var in = out.bytes().streamInput();
        in.setTransportVersion(EndpointMetadata.REGIONS_ADDED);
        var parsed = new EndpointClusterState(in);

        assertThat(parsed.endpointMetadata().heuristics(), is(heuristics));
        assertThat(parsed.endpointMetadata().internal(), is(internal));
        assertThat(in.available(), is(0));
    }

    /**
     * Verifies that writing an EndpointClusterState at the 9.4-era version produces bytes that an EndpointMetadata reader can
     * consume, with display/regions/deniedByRegionPolicy coming back as their empty/default values.
     */
    public void testWriteTo_LegacyVersion_9_4Era_ProducesFullLayoutForOldPeer() throws IOException {
        var service = randomAlphaOfLength(10);
        var clusterState = new EndpointClusterState(
            service,
            LEGACY_TEST_TASK_TYPE,
            null,
            null,
            null,
            EndpointMetadataClusterStateTests.randomNonEmptyInstance()
        );

        var out = new BytesStreamOutput();
        out.setTransportVersion(EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED);
        clusterState.writeTo(out);

        var in = out.bytes().streamInput();
        in.setTransportVersion(EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED);
        readNonMetadataPrefix(in);
        var fullMetadata = new EndpointMetadata(in);

        assertThat(fullMetadata.heuristics(), is(clusterState.endpointMetadata().heuristics()));
        assertThat(fullMetadata.internal(), is(clusterState.endpointMetadata().internal()));
        assertThat(fullMetadata.display(), is(EndpointMetadata.Display.EMPTY_INSTANCE));
        assertThat(fullMetadata.regions(), is(List.of()));
        assertFalse(fullMetadata.deniedByRegionPolicy());
        assertThat(in.available(), is(0));
    }

    /**
     * Verifies that writing an EndpointClusterState at the 9.5-era version produces bytes that an EndpointMetadata reader can
     * consume, with display/regions/deniedByRegionPolicy coming back as their empty/default values.
     */
    public void testWriteTo_LegacyVersion_9_5Era_ProducesFullLayoutForOldPeer() throws IOException {
        var service = randomAlphaOfLength(10);
        var clusterState = new EndpointClusterState(
            service,
            LEGACY_TEST_TASK_TYPE,
            null,
            null,
            null,
            EndpointMetadataClusterStateTests.randomNonEmptyInstance()
        );

        var out = new BytesStreamOutput();
        out.setTransportVersion(EndpointMetadata.REGIONS_ADDED);
        clusterState.writeTo(out);

        var in = out.bytes().streamInput();
        in.setTransportVersion(EndpointMetadata.REGIONS_ADDED);
        readNonMetadataPrefix(in);
        var fullMetadata = new EndpointMetadata(in);

        assertThat(fullMetadata.heuristics(), is(clusterState.endpointMetadata().heuristics()));
        assertThat(fullMetadata.internal(), is(clusterState.endpointMetadata().internal()));
        assertThat(fullMetadata.display(), is(EndpointMetadata.Display.EMPTY_INSTANCE));
        assertThat(fullMetadata.regions(), is(List.of()));
        assertFalse(fullMetadata.deniedByRegionPolicy());
        assertThat(in.available(), is(0));
    }
}
