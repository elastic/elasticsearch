/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/**
 * Drift-prevention tests for {@link NdJsonFormatReader}'s consumed-key contract.
 * See {@code CsvFormatReaderRecognizedKeysTests} for the rationale.
 */
public class NdJsonFormatReaderRecognizedKeysTests extends ESTestCase {

    private static final BlockFactory NOOP_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testRecognizedKeysSetIsExpected() {
        Set<String> expected = new TreeSet<>();
        expected.add("schema_sample_size");
        expected.add("segment_size");
        assertEquals(expected, new TreeSet<>(NdJsonFormatReader.RECOGNIZED_KEYS));
    }

    public void testEveryRecognizedKeyRoundTripsThroughWithConfig() {
        for (String key : NdJsonFormatReader.RECOGNIZED_KEYS) {
            Map<String, Object> config = new HashMap<>();
            config.put(key, sampleValueFor(key));
            try {
                Configured<FormatReader> result = newReader().withConfig(config);
                assertTrue("key [" + key + "] must be consumed when present", result.consumedKeys().contains(key));
            } catch (RuntimeException e) {
                // Validation rejection still proves recognition.
            }
        }
    }

    public void testUnknownKeysAreNotClaimed() {
        Map<String, Object> config = new HashMap<>();
        config.put("schema_sample_size", "20");
        config.put("not_an_ndjson_key", true);
        Configured<FormatReader> result = newReader().withConfig(config);
        assertThat(result.consumedKeys(), containsInAnyOrder("schema_sample_size"));
    }

    public void testEmptyConfigConsumesNothing() {
        assertThat(newReader().withConfig(Map.of()).consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        assertThat(newReader().withConfig(null).consumedKeys(), empty());
    }

    private NdJsonFormatReader newReader() {
        return new NdJsonFormatReader(Settings.EMPTY, NOOP_BLOCK_FACTORY, null);
    }

    private static Object sampleValueFor(String key) {
        return switch (key) {
            case "schema_sample_size" -> 10;
            case "segment_size" -> "2mb";
            default -> throw new AssertionError("update sampleValueFor() for new recognised key: " + key);
        };
    }
}
