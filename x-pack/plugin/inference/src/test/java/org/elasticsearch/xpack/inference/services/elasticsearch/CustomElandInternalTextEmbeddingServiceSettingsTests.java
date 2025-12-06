/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationSettingsTests;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.ELEMENT_TYPE;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.NUM_THREADS;
import static org.hamcrest.Matchers.is;

public class CustomElandInternalTextEmbeddingServiceSettingsTests extends AbstractWireSerializingTestCase<
    CustomElandInternalTextEmbeddingServiceSettings> {

    public static CustomElandInternalTextEmbeddingServiceSettings createRandom() {
        var withAdaptiveAllocations = randomBoolean();
        var numAllocations = withAdaptiveAllocations ? null : randomIntBetween(1, 10);
        var adaptiveAllocationsSettings = withAdaptiveAllocations
            ? new AdaptiveAllocationsSettings(true, randomIntBetween(0, 2), randomIntBetween(2, 5))
            : null;
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);
        var similarityMeasure = SimilarityMeasure.COSINE;
        var setDimensions = randomBoolean();
        var dims = setDimensions ? 123 : null;
        var elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());

        return new CustomElandInternalTextEmbeddingServiceSettings(
            numAllocations,
            numThreads,
            modelId,
            adaptiveAllocationsSettings,
            null,
            dims,
            similarityMeasure,
            elementType
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var modelId = "model-foo";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var numAllocations = 1;
        var numThreads = 1;
        var serviceSettings = CustomElandInternalTextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    NUM_ALLOCATIONS,
                    numAllocations,
                    NUM_THREADS,
                    numThreads,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ELEMENT_TYPE,
                    DenseVectorFieldMapper.ElementType.FLOAT.toString()
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new CustomElandInternalTextEmbeddingServiceSettings(
                    numAllocations,
                    numThreads,
                    modelId,
                    null,
                    null,
                    null,
                    SimilarityMeasure.DOT_PRODUCT,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            )
        );
    }

    public void testFromMap_Request_DoesNotDefaultSimilarityElementType() {
        var modelId = "model-foo";
        var numAllocations = 1;
        var numThreads = 1;
        var serviceSettings = CustomElandInternalTextEmbeddingServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId, NUM_ALLOCATIONS, numAllocations, NUM_THREADS, numThreads)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new CustomElandInternalTextEmbeddingServiceSettings(
                    numAllocations,
                    numThreads,
                    modelId,
                    null,
                    null,
                    null,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            )
        );
    }

    public void testFromMap_Request_IgnoresDimensions() {
        var modelId = "model-foo";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var numAllocations = 1;
        var numThreads = 1;
        var serviceSettings = CustomElandInternalTextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    NUM_ALLOCATIONS,
                    numAllocations,
                    NUM_THREADS,
                    numThreads,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ELEMENT_TYPE,
                    DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                    ServiceFields.DIMENSIONS,
                    1
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new CustomElandInternalTextEmbeddingServiceSettings(
                    numAllocations,
                    numThreads,
                    modelId,
                    null,
                    null,
                    null,
                    SimilarityMeasure.DOT_PRODUCT,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var modelId = "model-foo";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var numAllocations = 1;
        var numThreads = 1;
        var serviceSettings = CustomElandInternalTextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    NUM_ALLOCATIONS,
                    numAllocations,
                    NUM_THREADS,
                    numThreads,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ELEMENT_TYPE,
                    DenseVectorFieldMapper.ElementType.FLOAT.toString(),
                    ServiceFields.DIMENSIONS,
                    1
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CustomElandInternalTextEmbeddingServiceSettings(
                    numAllocations,
                    numThreads,
                    modelId,
                    null,
                    null,
                    1,
                    SimilarityMeasure.DOT_PRODUCT,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new CustomElandInternalTextEmbeddingServiceSettings(
            1,
            1,
            "model_id",
            null,
            null,
            100,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.BYTE
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"num_allocations":1,"num_threads":1,"model_id":"model_id","dimensions":100,"similarity":"cosine","element_type":"byte"}"""));
    }

    @Override
    protected Writeable.Reader<CustomElandInternalTextEmbeddingServiceSettings> instanceReader() {
        return CustomElandInternalTextEmbeddingServiceSettings::new;
    }

    @Override
    protected CustomElandInternalTextEmbeddingServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomElandInternalTextEmbeddingServiceSettings mutateInstance(CustomElandInternalTextEmbeddingServiceSettings instance)
        throws IOException {
        var numAllocations = instance.getNumAllocations();
        var numThreads = instance.getNumThreads();
        var modelId = instance.modelId();
        var adaptiveAllocationsSettings = instance.getAdaptiveAllocationsSettings();
        var deploymentId = instance.getDeploymentId();
        var dimensions = instance.dimensions();
        var similarity = instance.similarity();
        var elementType = instance.elementType();
        switch (randomInt(7)) {
            case 0 -> numAllocations = randomValueOtherThan(numAllocations, () -> randomFrom(randomIntBetween(1, 10), null));
            case 1 -> numThreads = randomValueOtherThan(numThreads, () -> randomIntBetween(1, 10));
            case 2 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 3 -> adaptiveAllocationsSettings = randomValueOtherThan(
                adaptiveAllocationsSettings,
                () -> randomFrom(AdaptiveAllocationSettingsTests.testInstance(), null)
            );
            case 4 -> deploymentId = randomValueOtherThan(deploymentId, () -> randomAlphaOfLengthOrNull(8));
            case 5 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 6 -> similarity = randomValueOtherThan(similarity, Utils::randomSimilarityMeasure);
            case 7 -> elementType = randomValueOtherThan(elementType, () -> randomFrom(DenseVectorFieldMapper.ElementType.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CustomElandInternalTextEmbeddingServiceSettings(
            numAllocations,
            numThreads,
            modelId,
            adaptiveAllocationsSettings,
            deploymentId,
            dimensions,
            similarity,
            elementType
        );
    }
}
