/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticRerankerServiceSettings.LONG_DOCUMENT_STRATEGY;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticRerankerServiceSettings.MAX_CHUNKS_PER_DOC;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.ADAPTIVE_ALLOCATIONS;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.NUM_THREADS;
import static org.hamcrest.Matchers.equalTo;

public class ElasticRerankerServiceSettingsTests extends AbstractElasticsearchInternalServiceSettingsTests<ElasticRerankerServiceSettings> {
    public static ElasticRerankerServiceSettings createRandomWithoutChunkingConfiguration(String modelId) {
        return createRandom(null, null, modelId);
    }

    public static ElasticRerankerServiceSettings createRandomWithChunkingConfiguration(
        ElasticRerankerServiceSettings.LongDocumentStrategy longDocumentStrategy,
        Integer maxChunksPerDoc
    ) {
        return createRandom(longDocumentStrategy, maxChunksPerDoc, randomAlphaOfLength(8));
    }

    public static ElasticRerankerServiceSettings createRandom() {
        var longDocumentStrategy = randomBoolean() ? randomFrom(ElasticRerankerServiceSettings.LongDocumentStrategy.values()) : null;
        var maxChunksPerDoc = ElasticRerankerServiceSettings.LongDocumentStrategy.CHUNK.equals(longDocumentStrategy) && randomBoolean()
            ? randomIntBetween(1, 10)
            : null;
        return createRandom(longDocumentStrategy, maxChunksPerDoc, randomAlphaOfLength(8));
    }

    private static ElasticRerankerServiceSettings createRandom(
        ElasticRerankerServiceSettings.LongDocumentStrategy longDocumentStrategy,
        Integer maxChunksPerDoc,
        String modelId
    ) {
        var withAdaptiveAllocations = randomBoolean();
        var numAllocations = withAdaptiveAllocations ? null : randomIntBetween(1, 10);
        var numThreads = randomIntBetween(1, 10);
        var adaptiveAllocationsSettings = withAdaptiveAllocations
            ? new AdaptiveAllocationsSettings(true, randomIntBetween(0, 2), randomIntBetween(2, 5))
            : null;
        return new ElasticRerankerServiceSettings(
            numAllocations,
            numThreads,
            modelId,
            adaptiveAllocationsSettings,
            longDocumentStrategy,
            maxChunksPerDoc
        );
    }

    public void testFromMap_NonAdaptiveAllocationsBaseSettings_CreatesSettingsCorrectly() {
        var numAllocations = randomIntBetween(1, 10);
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);

        Map<String, Object> settingsMap = buildServiceSettingsMap(
            Optional.of(numAllocations),
            numThreads,
            modelId,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );

        ElasticRerankerServiceSettings settings = ElasticRerankerServiceSettings.fromMap(settingsMap);
        assertExpectedSettings(
            settings,
            Optional.of(numAllocations),
            numThreads,
            modelId,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }

    public void testFromMap_AdaptiveAllocationsBaseSettings_CreatesSettingsCorrectly() {
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);
        var adaptiveAllocationsSettings = new AdaptiveAllocationsSettings(true, randomIntBetween(0, 2), randomIntBetween(2, 5));

        Map<String, Object> settingsMap = buildServiceSettingsMap(
            Optional.empty(),
            numThreads,
            modelId,
            Optional.of(adaptiveAllocationsSettings),
            Optional.empty(),
            Optional.empty()
        );

        ElasticRerankerServiceSettings settings = ElasticRerankerServiceSettings.fromMap(settingsMap);
        assertExpectedSettings(
            settings,
            Optional.empty(),
            numThreads,
            modelId,
            Optional.of(adaptiveAllocationsSettings),
            Optional.empty(),
            Optional.empty()
        );
    }

    public void testFromMap_NumAllocationsAndAdaptiveAllocationsNull_ThrowsValidationException() {
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);

        Map<String, Object> settingsMap = buildServiceSettingsMap(
            Optional.empty(),
            numThreads,
            modelId,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );

        ValidationException exception = Assert.assertThrows(
            ValidationException.class,
            () -> ElasticRerankerServiceSettings.fromMap(settingsMap)
        );

        assertTrue(
            exception.getMessage()
                .contains("[service_settings] does not contain one of the required settings [num_allocations, adaptive_allocations]")
        );
    }

    public void testFromMap_TruncateLongDocumentStrategySelected_CreatesSettingsCorrectly() {
        var withAdaptiveAllocations = randomBoolean();
        var numAllocations = withAdaptiveAllocations ? null : randomIntBetween(1, 10);
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);
        var adaptiveAllocationsSettings = withAdaptiveAllocations
            ? new AdaptiveAllocationsSettings(true, randomIntBetween(0, 2), randomIntBetween(2, 5))
            : null;
        var longDocumentStrategy = ElasticRerankerServiceSettings.LongDocumentStrategy.TRUNCATE;

        Map<String, Object> settingsMap = buildServiceSettingsMap(
            withAdaptiveAllocations ? Optional.empty() : Optional.of(numAllocations),
            numThreads,
            modelId,
            withAdaptiveAllocations ? Optional.of(adaptiveAllocationsSettings) : Optional.empty(),
            Optional.of(longDocumentStrategy),
            Optional.empty()
        );

        ElasticRerankerServiceSettings settings = ElasticRerankerServiceSettings.fromMap(settingsMap);
        assertExpectedSettings(
            settings,
            Optional.ofNullable(numAllocations),
            numThreads,
            modelId,
            Optional.ofNullable(adaptiveAllocationsSettings),
            Optional.of(longDocumentStrategy),
            Optional.empty()
        );
    }

    public void testFromMap_TruncateLongDocumentStrategySelectedWithMaxChunksPerDoc_ThrowsValidationException() {
        var withAdaptiveAllocations = randomBoolean();
        var numAllocations = withAdaptiveAllocations ? null : randomIntBetween(1, 10);
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);
        var adaptiveAllocationsSettings = withAdaptiveAllocations
            ? new AdaptiveAllocationsSettings(true, randomIntBetween(0, 2), randomIntBetween(2, 5))
            : null;
        var longDocumentStrategy = ElasticRerankerServiceSettings.LongDocumentStrategy.TRUNCATE;
        var maxChunksPerDoc = randomIntBetween(1, 10);

        Map<String, Object> settingsMap = buildServiceSettingsMap(
            withAdaptiveAllocations ? Optional.empty() : Optional.of(numAllocations),
            numThreads,
            modelId,
            withAdaptiveAllocations ? Optional.of(adaptiveAllocationsSettings) : Optional.empty(),
            Optional.of(longDocumentStrategy),
            Optional.of(maxChunksPerDoc)
        );

        ValidationException exception = Assert.assertThrows(
            ValidationException.class,
            () -> ElasticRerankerServiceSettings.fromMap(settingsMap)
        );

        assertTrue(
            exception.getMessage().contains("The [max_chunks_per_doc] setting requires [long_document_strategy] to be set to [chunk]")
        );
    }

    public void testFromMap_ChunkLongDocumentStrategySelected_CreatesSettingsCorrectly() {
        var withAdaptiveAllocations = randomBoolean();
        var numAllocations = withAdaptiveAllocations ? null : randomIntBetween(1, 10);
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);
        var adaptiveAllocationsSettings = withAdaptiveAllocations
            ? new AdaptiveAllocationsSettings(true, randomIntBetween(0, 2), randomIntBetween(2, 5))
            : null;
        var longDocumentStrategy = ElasticRerankerServiceSettings.LongDocumentStrategy.CHUNK;
        var maxChunksPerDoc = randomIntBetween(1, 10);

        Map<String, Object> settingsMap = buildServiceSettingsMap(
            withAdaptiveAllocations ? Optional.empty() : Optional.of(numAllocations),
            numThreads,
            modelId,
            withAdaptiveAllocations ? Optional.of(adaptiveAllocationsSettings) : Optional.empty(),
            Optional.of(longDocumentStrategy),
            Optional.of(maxChunksPerDoc)
        );

        ElasticRerankerServiceSettings settings = ElasticRerankerServiceSettings.fromMap(settingsMap);
        assertExpectedSettings(
            settings,
            Optional.ofNullable(numAllocations),
            numThreads,
            modelId,
            Optional.ofNullable(adaptiveAllocationsSettings),
            Optional.of(longDocumentStrategy),
            Optional.of(maxChunksPerDoc)
        );
    }

    public void testFromMap_ChunkLongDocumentStrategySelectedWithMaxChunksPerDoc_CreatesSettingsCorrectly() {
        var withAdaptiveAllocations = randomBoolean();
        var numAllocations = withAdaptiveAllocations ? null : randomIntBetween(1, 10);
        var numThreads = randomIntBetween(1, 10);
        var modelId = randomAlphaOfLength(8);
        var adaptiveAllocationsSettings = withAdaptiveAllocations
            ? new AdaptiveAllocationsSettings(true, randomIntBetween(0, 2), randomIntBetween(2, 5))
            : null;
        var longDocumentStrategy = ElasticRerankerServiceSettings.LongDocumentStrategy.CHUNK;
        var maxChunksPerDoc = randomIntBetween(1, 10);

        Map<String, Object> settingsMap = buildServiceSettingsMap(
            withAdaptiveAllocations ? Optional.empty() : Optional.of(numAllocations),
            numThreads,
            modelId,
            withAdaptiveAllocations ? Optional.of(adaptiveAllocationsSettings) : Optional.empty(),
            Optional.of(longDocumentStrategy),
            Optional.of(maxChunksPerDoc)
        );

        ElasticRerankerServiceSettings settings = ElasticRerankerServiceSettings.fromMap(settingsMap);
        assertExpectedSettings(
            settings,
            Optional.ofNullable(numAllocations),
            numThreads,
            modelId,
            Optional.ofNullable(adaptiveAllocationsSettings),
            Optional.of(longDocumentStrategy),
            Optional.of(maxChunksPerDoc)
        );
    }

    private Map<String, Object> buildServiceSettingsMap(
        Optional<Integer> numAllocations,
        int numThreads,
        String modelId,
        Optional<AdaptiveAllocationsSettings> adaptiveAllocationsSettings,
        Optional<ElasticRerankerServiceSettings.LongDocumentStrategy> longDocumentStrategy,
        Optional<Integer> maxChunksPerDoc
    ) {
        var settingsMap = new HashMap<String, Object>();
        numAllocations.ifPresent(value -> settingsMap.put(NUM_ALLOCATIONS, value));
        settingsMap.put(NUM_THREADS, numThreads);
        settingsMap.put(MODEL_ID, modelId);
        adaptiveAllocationsSettings.ifPresent(settings -> {
            var adaptiveMap = new HashMap<String, Object>();
            adaptiveMap.put(AdaptiveAllocationsSettings.ENABLED.getPreferredName(), settings.getEnabled());
            adaptiveMap.put(AdaptiveAllocationsSettings.MIN_NUMBER_OF_ALLOCATIONS.getPreferredName(), settings.getMinNumberOfAllocations());
            adaptiveMap.put(AdaptiveAllocationsSettings.MAX_NUMBER_OF_ALLOCATIONS.getPreferredName(), settings.getMaxNumberOfAllocations());
            settingsMap.put(ADAPTIVE_ALLOCATIONS, adaptiveMap);
        });
        longDocumentStrategy.ifPresent(value -> settingsMap.put(LONG_DOCUMENT_STRATEGY, value.toString()));
        maxChunksPerDoc.ifPresent(value -> settingsMap.put(MAX_CHUNKS_PER_DOC, value));
        return settingsMap;
    }

    private void assertExpectedSettings(
        ElasticRerankerServiceSettings settings,
        Optional<Integer> expectedNumAllocations,
        int expectedNumThreads,
        String expectedModelId,
        Optional<AdaptiveAllocationsSettings> expectedAdaptiveAllocationsSettings,
        Optional<ElasticRerankerServiceSettings.LongDocumentStrategy> expectedLongDocumentStrategy,
        Optional<Integer> expectedMaxChunksPerDoc
    ) {
        assertEquals(expectedNumAllocations.orElse(null), settings.getNumAllocations());
        assertEquals(expectedNumThreads, settings.getNumThreads());
        assertEquals(expectedModelId, settings.modelId());
        assertEquals(expectedAdaptiveAllocationsSettings.orElse(null), settings.getAdaptiveAllocationsSettings());
        assertEquals(expectedLongDocumentStrategy.orElse(null), settings.getLongDocumentStrategy());
        assertEquals(expectedMaxChunksPerDoc.orElse(null), settings.getMaxChunksPerDoc());
    }

    @Override
    protected Writeable.Reader<ElasticRerankerServiceSettings> instanceReader() {
        return ElasticRerankerServiceSettings::new;
    }

    @Override
    protected ElasticRerankerServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticRerankerServiceSettings mutateInstance(ElasticRerankerServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, ElasticRerankerServiceSettingsTests::createRandom);
    }

    @Override
    protected void assertUpdated(ElasticRerankerServiceSettings original, ElasticRerankerServiceSettings updated) {
        assertThat(updated.getLongDocumentStrategy(), equalTo(original.getLongDocumentStrategy()));
        assertThat(updated.getMaxChunksPerDoc(), equalTo(original.getMaxChunksPerDoc()));
    }
}
