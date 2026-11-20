/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;

/**
 * Tests for {@link Warnings} and their collection into a {@link DriverContext}.
 */
public class WarningsTests extends ESTestCase {
    public void testRegisterCollect() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(1, 1, "foo");
        warnings.registerException(new IllegalArgumentException());
        assertThat(
            collected(dc),
            contains(
                "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: null"
            )
        );
    }

    public void testRegisterCollectFilled() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(1, 1, "foo");
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1: java.lang.IllegalArgumentException: " + i;
        }

        assertThat(collected(dc), contains(expected));
    }

    private static DriverContext collectingContext() {
        BlockFactory blockFactory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory);
    }

    private static List<String> collected(DriverContext dc) {
        dc.finish();
        return dc.warnings();
    }
}
