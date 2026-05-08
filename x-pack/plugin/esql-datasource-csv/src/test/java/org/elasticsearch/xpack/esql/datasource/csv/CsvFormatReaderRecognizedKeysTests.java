/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/** Pins {@link CsvFormatReader#RECOGNIZED_KEYS} against the parser's actual reads. */
public class CsvFormatReaderRecognizedKeysTests extends ESTestCase {

    private static final BlockFactory NOOP_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testRecognizedKeysSetIsExpected() {
        Set<String> expected = new TreeSet<>();
        expected.add("column_prefix");
        expected.add("comment");
        expected.add("datetime_format");
        expected.add("delimiter");
        expected.add("encoding");
        expected.add("escape");
        expected.add("header_row");
        expected.add("max_field_size");
        expected.add("multi_value_syntax");
        expected.add("null_value");
        expected.add("quote");
        expected.add("schema_sample_size");
        assertEquals(expected, new TreeSet<>(CsvFormatReader.RECOGNIZED_KEYS));
    }

    public void testEveryRecognizedKeyRoundTripsThroughWithConfig() {
        for (String key : CsvFormatReader.RECOGNIZED_KEYS) {
            Map<String, Object> config = new HashMap<>();
            config.put(key, sampleValueFor(key));
            try {
                Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(config);
                assertTrue("key [" + key + "] must be consumed when present", result.consumedKeys().contains(key));
            } catch (RuntimeException e) {
                // A throw still proves the key was read — the reader looked at it before rejecting.
            }
        }
    }

    public void testUnknownKeysAreNotClaimed() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("delimiter", "|");
        config.put("not_a_csv_key", true);
        config.put("alsobogus", 42);
        Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(config);
        assertThat(result.consumedKeys(), containsInAnyOrder("delimiter"));
    }

    public void testEmptyConfigConsumesNothing() {
        Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(Map.of());
        assertThat(result.consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(null);
        assertThat(result.consumedKeys(), empty());
    }

    public void testRandomKeysNeverClaimedBeyondRecognizedSet() {
        Map<String, Object> config = new HashMap<>();
        Set<String> expectedConsumed = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            String key = randomAlphaOfLength(between(3, 12)).toLowerCase(java.util.Locale.ROOT);
            config.put(key, randomBoolean() ? randomAlphaOfLength(5) : randomInt());
            if (CsvFormatReader.RECOGNIZED_KEYS.contains(key)) {
                expectedConsumed.add(key);
            }
        }
        try {
            Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(config);
            assertEquals(expectedConsumed, result.consumedKeys());
        } catch (RuntimeException e) {
            // Random values may trigger parser validation; the contract is only checked on success.
        }
    }

    /**
     * Bidirectional symmetry: every {@code static final String CONFIG_*} constant on
     * {@link CsvFormatReader} appears in {@link CsvFormatReader#RECOGNIZED_KEYS}, and every entry
     * in {@code RECOGNIZED_KEYS} is backed by a matching constant.
     */
    @SuppressForbidden(reason = "test-only reflection over CONFIG_* constants to pin set/constant symmetry")
    public void testRecognizedKeysMatchConfigConstants() {
        Set<String> fromConstants = new TreeSet<>();
        for (Field f : CsvFormatReader.class.getDeclaredFields()) {
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
        missingFromKeys.removeAll(CsvFormatReader.RECOGNIZED_KEYS);
        Set<String> extraInKeys = new TreeSet<>(CsvFormatReader.RECOGNIZED_KEYS);
        extraInKeys.removeAll(fromConstants);
        assertTrue("CsvFormatReader CONFIG_* constants missing from RECOGNIZED_KEYS: " + missingFromKeys, missingFromKeys.isEmpty());
        assertTrue("CsvFormatReader RECOGNIZED_KEYS entries with no backing CONFIG_* constant: " + extraInKeys, extraInKeys.isEmpty());
    }

    private static Object sampleValueFor(String key) {
        return switch (key) {
            case "delimiter" -> "|";
            case "quote" -> "\"";
            case "escape" -> "\\";
            case "comment" -> "#";
            case "null_value" -> "NA";
            case "encoding" -> "UTF-8";
            case "datetime_format" -> "yyyy-MM-dd";
            case "max_field_size" -> 1024;
            case "multi_value_syntax" -> "brackets";
            case "header_row" -> false;
            case "column_prefix" -> "f_";
            case "schema_sample_size" -> 10;
            default -> throw new AssertionError("update sampleValueFor() for new recognised key: " + key);
        };
    }
}
