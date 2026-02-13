/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class ChunkScorerConfigTests extends ESTestCase {

    public void testChunkingSettingsFromMapReturnsNullForNull() {
        assertNull(ChunkScorerConfig.chunkingSettingsFromMap(null));
    }

    public void testChunkingSettingsFromMapWithOnlyMaxChunkSize() {
        Map<String, Object> map = Map.of("max_chunk_size", 500);
        ChunkingSettings settings = ChunkScorerConfig.chunkingSettingsFromMap(map);

        assertNotNull(settings);
        assertEquals(500, settings.asMap().get("max_chunk_size"));
        assertEquals("sentence", settings.asMap().get("strategy"));
    }

    public void testChunkingSettingsFromMapWithFullSettings() {
        Map<String, Object> map = Map.of("strategy", "sentence", "max_chunk_size", 200, "sentence_overlap", 1);
        ChunkingSettings settings = ChunkScorerConfig.chunkingSettingsFromMap(map);

        assertNotNull(settings);
        assertEquals(200, settings.asMap().get("max_chunk_size"));
    }

    public void testDefaultChunkingSettingsWithValidSize() {
        ChunkingSettings settings = ChunkScorerConfig.defaultChunkingSettings(500);

        assertNotNull(settings);
        assertEquals(500, settings.asMap().get("max_chunk_size"));
        assertEquals("sentence", settings.asMap().get("strategy"));
    }

    public void testChunkingSettingsFromMapWithNullMaxChunkSizeThrows() {
        Map<String, Object> map = new java.util.HashMap<>();
        map.put("max_chunk_size", null);
        expectThrows(IllegalArgumentException.class, () -> ChunkScorerConfig.chunkingSettingsFromMap(map));
    }

    public void testSerializationRoundTripWithChunkingSettings() throws IOException {
        ChunkScorerConfig original = new ChunkScorerConfig(3, "test query", ChunkScorerConfig.defaultChunkingSettings(500));

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ChunkScorerConfig deserialized = new ChunkScorerConfig(in);

        assertEquals(original.size(), deserialized.size());
        assertEquals(original.inferenceText(), deserialized.inferenceText());
        assertNotNull(deserialized.chunkingSettings());
        assertEquals(500, deserialized.chunkingSettings().asMap().get("max_chunk_size"));
    }

    public void testSerializationRoundTripWithNullChunkingSettings() throws IOException {
        ChunkScorerConfig original = new ChunkScorerConfig(3, "test query", null);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ChunkScorerConfig deserialized = new ChunkScorerConfig(in);

        assertEquals(original.size(), deserialized.size());
        assertEquals(original.inferenceText(), deserialized.inferenceText());
        assertNull(deserialized.chunkingSettings());
    }

    public void testSerializationRoundTripWithNullSize() throws IOException {
        ChunkScorerConfig original = new ChunkScorerConfig(null, "test query", ChunkScorerConfig.defaultChunkingSettings(200));

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ChunkScorerConfig deserialized = new ChunkScorerConfig(in);

        assertNull(deserialized.size());
        assertEquals("test query", deserialized.inferenceText());
        assertNotNull(deserialized.chunkingSettings());
    }

    public void testSizeOrDefaultWithSize() {
        ChunkScorerConfig config = new ChunkScorerConfig(5, "query", null);
        assertEquals(5, config.sizeOrDefault());
    }

    public void testSizeOrDefaultWithNull() {
        ChunkScorerConfig config = new ChunkScorerConfig(null, "query", null);
        assertEquals(ChunkScorerConfig.DEFAULT_SIZE, config.sizeOrDefault());
    }
}
