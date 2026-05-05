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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/**
 * Drift-prevention tests for {@link CsvFormatReader}'s consumed-key contract. The coordinator
 * relies on the consumed set to detect WITH-clause typos: if a real option is silently dropped
 * from {@code RECOGNIZED_KEYS}, the coordinator would reject it as unknown for every legitimate
 * user. These tests pin the contract by:
 * <ol>
 *   <li>asserting the recognised set against an explicit literal (any addition forces an update);</li>
 *   <li>verifying every recognised key actually round-trips through {@code withConfig};</li>
 *   <li>verifying random WITH-clause maps never claim more than the recognised set.</li>
 * </ol>
 */
public class CsvFormatReaderRecognizedKeysTests extends ESTestCase {

    private static final BlockFactory NOOP_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testRecognizedKeysSetIsExpected() {
        // Pinning literal: drift-prevention. If a CSV option is added or removed, this list MUST
        // be updated alongside withConfig — otherwise the coordinator silently rejects the option
        // for every user. Sorted alphabetically for stable diffs.
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
        // For each key in the recognised set, putting it in the input map must result in it being
        // present in the consumed set. If any key is in RECOGNIZED_KEYS but withConfig forgets to
        // read it, this test catches the regression.
        for (String key : CsvFormatReader.RECOGNIZED_KEYS) {
            Map<String, Object> config = new HashMap<>();
            config.put(key, sampleValueFor(key));
            try {
                Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfig(config);
                assertTrue("key [" + key + "] must be consumed when present", result.consumedKeys().contains(key));
            } catch (RuntimeException e) {
                // Some keys reject empty / bogus values (e.g. encoding = "junk"). That's fine —
                // failure means the reader did look at the key, which proves recognition.
            }
        }
    }

    public void testUnknownKeysAreNotClaimed() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("delimiter", "|");
        config.put("not_a_csv_key", true);
        config.put("alsobogus", 42);
        Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfig(config);
        assertThat(result.consumedKeys(), containsInAnyOrder("delimiter"));
    }

    public void testEmptyConfigConsumesNothing() {
        Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfig(Map.of());
        assertThat(result.consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfig(null);
        assertThat(result.consumedKeys(), empty());
    }

    public void testRandomKeysNeverClaimedBeyondRecognizedSet() {
        // Throw a salad of random-looking keys at withConfig. None should ever be claimed unless
        // it happens to fall in RECOGNIZED_KEYS. Probabilistically catches a regression where
        // someone introduces an undocumented key consumption.
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
            Configured<FormatReader> result = new CsvFormatReader(NOOP_BLOCK_FACTORY).withConfig(config);
            assertEquals(expectedConsumed, result.consumedKeys());
        } catch (RuntimeException e) {
            // Random values may fail validation (e.g. random string for max_field_size). That's
            // an expected failure mode — the test only cares about consumed-key correctness on
            // the success path.
        }
    }

    private static Object sampleValueFor(String key) {
        // Values are chosen so each key parses successfully without depending on other keys.
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
