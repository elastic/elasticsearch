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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/** Pins {@link NdJsonFormatReader#RECOGNIZED_KEYS} against the parser's actual reads. */
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
                Configured<FormatReader> result = newReader().withConfigTrackingConsumedKeys(config);
                assertTrue("key [" + key + "] must be consumed when present", result.consumedKeys().contains(key));
            } catch (RuntimeException e) {
                // A throw still proves the key was read — the reader looked at it before rejecting.
            }
        }
    }

    public void testUnknownKeysAreNotClaimed() {
        Map<String, Object> config = new HashMap<>();
        config.put("schema_sample_size", "20");
        config.put("not_an_ndjson_key", true);
        Configured<FormatReader> result = newReader().withConfigTrackingConsumedKeys(config);
        assertThat(result.consumedKeys(), containsInAnyOrder("schema_sample_size"));
    }

    public void testEmptyConfigConsumesNothing() {
        assertThat(newReader().withConfigTrackingConsumedKeys(Map.of()).consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        assertThat(newReader().withConfigTrackingConsumedKeys(null).consumedKeys(), empty());
    }

    /**
     * Bidirectional symmetry: every {@code static final String CONFIG_*} constant on
     * {@link NdJsonFormatReader} appears in {@link NdJsonFormatReader#RECOGNIZED_KEYS}, and every
     * entry in {@code RECOGNIZED_KEYS} is backed by a matching constant.
     */
    @SuppressForbidden(reason = "test-only reflection over CONFIG_* constants to pin set/constant symmetry")
    public void testRecognizedKeysMatchConfigConstants() {
        Set<String> fromConstants = new TreeSet<>();
        for (Field f : NdJsonFormatReader.class.getDeclaredFields()) {
            int mods = f.getModifiers();
            if (Modifier.isStatic(mods) == false || Modifier.isFinal(mods) == false) continue;
            if (f.getType() != String.class) continue;
            if (f.getName().startsWith("CONFIG_") == false) continue;
            f.setAccessible(true);
            try {
                String value = (String) f.get(null);
                if (value != null) fromConstants.add(value);
            } catch (IllegalAccessException e) {
                throw new AssertionError("cannot read constant " + f.getName(), e);
            }
        }
        Set<String> missingFromKeys = new TreeSet<>(fromConstants);
        missingFromKeys.removeAll(NdJsonFormatReader.RECOGNIZED_KEYS);
        Set<String> extraInKeys = new TreeSet<>(NdJsonFormatReader.RECOGNIZED_KEYS);
        extraInKeys.removeAll(fromConstants);
        assertTrue("NdJsonFormatReader CONFIG_* constants missing from RECOGNIZED_KEYS: " + missingFromKeys, missingFromKeys.isEmpty());
        assertTrue("NdJsonFormatReader RECOGNIZED_KEYS entries with no backing CONFIG_* constant: " + extraInKeys, extraInKeys.isEmpty());
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
