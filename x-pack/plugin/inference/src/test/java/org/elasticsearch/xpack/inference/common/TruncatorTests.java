/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class TruncatorTests extends ESTestCase {
    public void testTruncate_ReducesLengthByHalf() {
        assertThat(Truncator.truncate(List.of("123456", "awesome"), 0.5), is(List.of("123", "awe")));
    }

    public void testTruncate_ReducesLengthToZero() {
        assertThat(Truncator.truncate(List.of("1"), 0.5), is(List.of("")));
    }

    public void testTruncate_ReturnsAnEmptyString_WhenItIsAnEmptyString() {
        assertThat(Truncator.truncate(List.of(""), 0.5), is(List.of("")));
    }

    public void testTruncate_ThrowsIfPercentageIsGreaterThan1() {
        expectThrows(AssertionError.class, () -> Truncator.truncate(List.of(), 1.001));
    }

    public void testTruncate_ThrowsIfPercentageIs1() {
        expectThrows(AssertionError.class, () -> Truncator.truncate(List.of(), 1));
    }

    public void testTruncate_ThrowsIfPercentageIs0() {
        expectThrows(AssertionError.class, () -> Truncator.truncate(List.of(), 0));
    }

    public void testTruncate_ThrowsIfPercentageIsLessThan0() {
        expectThrows(AssertionError.class, () -> Truncator.truncate(List.of(), -0.001));
    }
}
