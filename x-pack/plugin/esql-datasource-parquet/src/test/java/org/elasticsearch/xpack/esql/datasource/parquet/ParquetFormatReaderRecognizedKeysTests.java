/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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

/** Pins {@link ParquetFormatReader#RECOGNIZED_KEYS} against the parser's actual reads. */
public class ParquetFormatReaderRecognizedKeysTests extends ESTestCase {

    private static final BlockFactory NOOP_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testRecognizedKeysSetIsExpected() {
        Set<String> expected = new TreeSet<>();
        expected.add("optimized_reader");
        expected.add("late_materialization");
        assertEquals(expected, new TreeSet<>(ParquetFormatReader.RECOGNIZED_KEYS));
    }

    public void testEveryRecognizedKeyRoundTripsThroughWithConfig() {
        for (String key : ParquetFormatReader.RECOGNIZED_KEYS) {
            Map<String, Object> config = new HashMap<>();
            config.put(key, true);
            Configured<FormatReader> result = new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(config);
            assertTrue("key [" + key + "] must be consumed when present", result.consumedKeys().contains(key));
        }
    }

    public void testUnknownKeysAreNotClaimed() {
        Map<String, Object> config = new HashMap<>();
        config.put("optimized_reader", true);
        config.put("not_a_parquet_key", true);
        Configured<FormatReader> result = new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(config);
        assertThat(result.consumedKeys(), containsInAnyOrder("optimized_reader"));
    }

    public void testEmptyConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(Map.of()).consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(null).consumedKeys(), empty());
    }
}
