/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class MinHashIndexParamBoundsTests extends ESTestCase {

    public void testWithinBounds() {
        assertNull(MinHashIndexParamBounds.violation(100, 100, 1));
        assertNull(MinHashIndexParamBounds.violation(1, 512, 195));
        // Exactly at both limits: hash_count * bucket_count = 10,000 and the full product = 100,000.
        assertNull(MinHashIndexParamBounds.violation(1, 10_000, 10));
    }

    public void testHashSetsBound() {
        assertEquals(
            "The product of hash_count [10001] and bucket_count [1] is [10001], which exceeds the limit [10000].",
            MinHashIndexParamBounds.violation(10_001, 1, 1)
        );
    }

    public void testHashEntriesBound() {
        assertEquals(
            "The product of hash_count [1], bucket_count [512], and hash_set_size [196] is [100352], which exceeds the limit [100000].",
            MinHashIndexParamBounds.violation(1, 512, 196)
        );
    }

    public void testLargeValuesDoNotOverflow() {
        // The product of all three wraps to zero in long arithmetic.
        assertEquals(
            "The product of hash_count [2097152] and bucket_count [2097152] is [4398046511104], which exceeds the limit [10000].",
            MinHashIndexParamBounds.violation(2097152, 2097152, 4194304)
        );
        assertEquals(
            "The product of hash_count [2147483647] and bucket_count [2147483647] is [4611686014132420609], "
                + "which exceeds the limit [10000].",
            MinHashIndexParamBounds.violation(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE)
        );
    }

    public void testFilterSettingsViolation() {
        // The defaults are 1 * 512 * 1.
        assertNull(MinHashIndexParamBounds.filterSettingsViolation(Settings.EMPTY));
        assertEquals(
            MinHashIndexParamBounds.violation(1, 512, 200),
            MinHashIndexParamBounds.filterSettingsViolation(Settings.builder().put("hash_set_size", 200).build())
        );
    }

    public void testFilterSettingsViolationUnparseableSettings() {
        for (String key : List.of("hash_count", "bucket_count", "hash_set_size")) {
            SettingsException e = expectThrows(
                SettingsException.class,
                () -> MinHashIndexParamBounds.filterSettingsViolation(Settings.builder().put(key, "not_a_number").build())
            );
            assertEquals("Failed to parse int setting [" + key + "] with value [not_a_number]", e.getMessage());
        }
    }

    public void testIndexSettingsViolations() {
        assertEquals(List.of(), MinHashIndexParamBounds.indexSettingsViolations(null));
        assertEquals(List.of(), MinHashIndexParamBounds.indexSettingsViolations(Settings.EMPTY));

        // Only the min_hash filter that exceeds the bounds is reported, not one within them, one that can't be parsed, or a filter of
        // another type.
        Settings indexSettings = Settings.builder()
            .put("index.analysis.filter.big.type", "min_hash")
            .put("index.analysis.filter.big.hash_count", 1)
            .put("index.analysis.filter.big.bucket_count", 512)
            .put("index.analysis.filter.big.hash_set_size", 200)
            .put("index.analysis.filter.small.type", "min_hash")
            .put("index.analysis.filter.small.hash_set_size", 20)
            .put("index.analysis.filter.bad.type", "min_hash")
            .put("index.analysis.filter.bad.hash_count", "not_a_number")
            .put("index.analysis.filter.other.type", "lowercase")
            .build();
        assertEquals(
            List.of("[big]: " + MinHashIndexParamBounds.violation(1, 512, 200)),
            MinHashIndexParamBounds.indexSettingsViolations(indexSettings)
        );
    }
}
