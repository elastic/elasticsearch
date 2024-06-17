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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.ELEMENT_TYPE;
import static org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings.NUM_ALLOCATIONS;
import static org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings.NUM_THREADS;
import static org.hamcrest.Matchers.is;

public class CustomElandInternalServiceSettingsTests extends AbstractWireSerializingTestCase<CustomElandInternalServiceSettings> {

    public static CustomElandInternalServiceSettings createRandom() {
        var numAllocations = randomIntBetween(1, 10);
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.COSINE;
            dims = 123;
        }

        var elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());

        return new CustomElandInternalServiceSettings(numAllocations, numThreads, modelId, dims, similarityMeasure, elementType);
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var modelId = "model-foo";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var numAllocations = 1;
        var numThreads = 1;
        var serviceSettings = CustomElandInternalServiceSettings.fromMap(
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
                new CustomElandInternalServiceSettings(
                    numAllocations,
                    numThreads,
                    modelId,
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
        var serviceSettings = CustomElandInternalServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId, NUM_ALLOCATIONS, numAllocations, NUM_THREADS, numThreads)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new CustomElandInternalServiceSettings(numAllocations, numThreads, modelId, null, null, null)));
    }

    public void testFromMap_Request_IgnoresDimensions() {
        var modelId = "model-foo";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var numAllocations = 1;
        var numThreads = 1;
        var serviceSettings = CustomElandInternalServiceSettings.fromMap(
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
                new CustomElandInternalServiceSettings(
                    numAllocations,
                    numThreads,
                    modelId,
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
        var serviceSettings = CustomElandInternalServiceSettings.fromMap(
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
                new CustomElandInternalServiceSettings(
                    numAllocations,
                    numThreads,
                    modelId,
                    1,
                    SimilarityMeasure.DOT_PRODUCT,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new CustomElandInternalServiceSettings(
            1,
            1,
            "model_id",
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
    protected Writeable.Reader<CustomElandInternalServiceSettings> instanceReader() {
        return CustomElandInternalServiceSettings::new;
    }

    @Override
    protected CustomElandInternalServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomElandInternalServiceSettings mutateInstance(CustomElandInternalServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, CustomElandInternalServiceSettingsTests::createRandom);
    }
}
