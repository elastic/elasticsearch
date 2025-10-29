/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class RerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<RerankServiceSettings> {

    public static RerankServiceSettings createRandom() {
        LongDocumentStrategy longDocumentStrategy = randomBoolean() ? null : randomFrom(LongDocumentStrategy.values());
        Integer maxChunksPerDoc = longDocumentStrategy == null ? null : randomBoolean() ? null : randomNonNegativeInt();
        return new RerankServiceSettings(longDocumentStrategy, maxChunksPerDoc);
    }

    public void testFromMap_MapDoesNotIncludeValues_CreatesRerankServiceSettings() {
        Map<String, Object> map = new HashMap<>();
        RerankServiceSettings settings = RerankServiceSettings.fromMap(map);
        assertNull(settings.getLongDocumentStrategy());
        assertNull(settings.getMaxChunksPerDoc());
    }

    public void testFromMap_AllValuesSetToNull_CreatesRerankServiceSettings() {
        Map<String, Object> map = new HashMap<>();
        map.put(RerankServiceSettings.LONG_DOCUMENT_STRATEGY, null);
        map.put(RerankServiceSettings.MAX_CHUNKS_PER_DOC, null);
        RerankServiceSettings settings = RerankServiceSettings.fromMap(map);
        assertNull(settings.getLongDocumentStrategy());
        assertNull(settings.getMaxChunksPerDoc());
    }

    public void testFromMap_LongDocumentStrategyNullAndMaxChunksPerDocProvided_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(RerankServiceSettings.LONG_DOCUMENT_STRATEGY, null);
        map.put(RerankServiceSettings.MAX_CHUNKS_PER_DOC, randomNonNegativeInt());
        ValidationException exception = expectThrows(ValidationException.class, () -> RerankServiceSettings.fromMap(map));
        assertThat(
            exception.getMessage(),
            containsString("Setting [max_chunks_per_doc] cannot be set without setting [long_document_strategy]")
        );
    }

    public void testFromMap_LongDocumentStrategyTruncate_CreatesRerankServiceSettings() {
        Map<String, Object> map = new HashMap<>();
        map.put(RerankServiceSettings.LONG_DOCUMENT_STRATEGY, LongDocumentStrategy.TRUNCATE.strategyName);
        RerankServiceSettings settings = RerankServiceSettings.fromMap(map);
        assertEquals(LongDocumentStrategy.TRUNCATE, settings.getLongDocumentStrategy());
        assertNull(settings.getMaxChunksPerDoc());
    }

    public void testFromMap_LongDocumentStrategyTruncateAndMaxChunksPerDocProvided_ThrowsValidationException() {
        Map<String, Object> map = new HashMap<>();
        map.put(RerankServiceSettings.LONG_DOCUMENT_STRATEGY, LongDocumentStrategy.TRUNCATE.strategyName);
        map.put(RerankServiceSettings.MAX_CHUNKS_PER_DOC, 5);
        ValidationException exception = expectThrows(ValidationException.class, () -> RerankServiceSettings.fromMap(map));
        assertThat(
            exception.getMessage(),
            containsString("Setting [max_chunks_per_doc] cannot be set without setting [long_document_strategy]")
        );
    }

    public void testFromMap_LongDocumentStrategyWithMaxChunksNotProvided_CreatesRerankServiceSettings() {
        Map<String, Object> map = new HashMap<>();
        map.put(RerankServiceSettings.LONG_DOCUMENT_STRATEGY, LongDocumentStrategy.CHUNK.strategyName);
        RerankServiceSettings settings = RerankServiceSettings.fromMap(map);
        assertEquals(LongDocumentStrategy.CHUNK, settings.getLongDocumentStrategy());
        assertNull(settings.getMaxChunksPerDoc());
    }

    public void testFromMap_LongDocumentStrategyWithMaxChunksProvided_CreatesRerankServiceSettings() {
        Map<String, Object> map = new HashMap<>();
        map.put(RerankServiceSettings.LONG_DOCUMENT_STRATEGY, LongDocumentStrategy.CHUNK.strategyName);
        int maxChunksPerDoc = randomNonNegativeInt();
        map.put(RerankServiceSettings.MAX_CHUNKS_PER_DOC, maxChunksPerDoc);
        RerankServiceSettings settings = RerankServiceSettings.fromMap(map);
        assertEquals(LongDocumentStrategy.CHUNK, settings.getLongDocumentStrategy());
        assertEquals(maxChunksPerDoc, settings.getMaxChunksPerDoc().intValue());
    }

    @Override
    protected RerankServiceSettings mutateInstanceForVersion(RerankServiceSettings instance, TransportVersion version) {
        // TODO: Update this when transport version is added.
        // No changes across versions yet
        return instance;
    }

    @Override
    protected Writeable.Reader<RerankServiceSettings> instanceReader() {
        return RerankServiceSettings::new;
    }

    @Override
    protected RerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected RerankServiceSettings mutateInstance(RerankServiceSettings instance) throws IOException {
        LongDocumentStrategy longDocumentStrategy = instance.getLongDocumentStrategy();
        Integer maxChunksPerDoc = instance.getMaxChunksPerDoc();
        switch (randomInt(1)) {
            case 0 -> {
                longDocumentStrategy = randomValueOtherThan(longDocumentStrategy, () -> randomFrom(LongDocumentStrategy.values()));
            }
            case 1 -> {
                maxChunksPerDoc = randomValueOtherThan(maxChunksPerDoc, ESTestCase::randomNonNegativeInt);
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new RerankServiceSettings(longDocumentStrategy, maxChunksPerDoc);
    }
}
