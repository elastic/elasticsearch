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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class MinimalServiceSettingsTests extends AbstractBWCSerializationTestCase<MinimalServiceSettings> {

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
        return createRandom();
    }

    public static MinimalServiceSettings createRandom() {
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

        boolean isEmbeddingTask = taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.EMBEDDING;

        switch (randomIntBetween(0, 5)) {
            case 0 -> {
                if (service == null) {
                    service = randomAlphaOfLength(10);
                } else {
                    service = null;
                }
            }
            case 1 -> {
                taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
                // Update dimensions, similarity, elementType based on new taskType
                if (taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.EMBEDDING) {
                    dimensions = randomIntBetween(2, 1024);
                    similarity = randomFrom(SimilarityMeasure.values());
                    elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
                } else {
                    dimensions = null;
                    similarity = null;
                    elementType = null;
                }
            }
            case 2 -> {
                if (isEmbeddingTask) {
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
                if (isEmbeddingTask) {
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
                if (isEmbeddingTask) {
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
                    // Generate a non-EMPTY instance by ensuring at least one field is non-empty
                    var heuristics = EndpointMetadataTests.randomHeuristics();
                    var internal = EndpointMetadataTests.randomInternal();
                    var display = new EndpointMetadata.Display(randomAlphaOfLengthBetween(1, 20));
                    endpointMetadata = new EndpointMetadata(heuristics, internal, display);
                } else {
                    endpointMetadata = randomValueOtherThan(
                        endpointMetadata,
                        () -> randomBoolean() ? EndpointMetadata.EMPTY_INSTANCE : EndpointMetadataTests.randomInstance()
                    );
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
}
