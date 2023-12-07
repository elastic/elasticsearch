/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.common.Truncator.truncate;
import static org.hamcrest.Matchers.is;

public class TruncatorTests extends ESTestCase {

    public void testTruncate_Percentage_ReducesLengthByHalf() {
        var truncator = createTruncator();
        assertThat(
            truncator.truncate(List.of("123456", "awesome")),
            is(new Truncator.TruncationResult(List.of("123", "awe"), List.of(true, true)))
        );
    }

    public void testTruncate_Percentage_OnlyTruncatesTheFirstEntry() {
        var truncator = createTruncator();
        assertThat(truncator.truncate(List.of("123456", "")), is(new Truncator.TruncationResult(List.of("123", ""), List.of(true, false))));
    }

    public void testTruncate_Percentage_ReducesLengthToZero() {
        var truncator = createTruncator();
        assertThat(truncator.truncate(List.of("1")), is(new Truncator.TruncationResult(List.of(""), List.of(true))));
    }

    public void testTruncate_Percentage_ReturnsAnEmptyString_WhenItIsAnEmptyString() {
        var truncator = createTruncator();
        assertThat(truncator.truncate(List.of("")), is(new Truncator.TruncationResult(List.of(""), List.of(false))));
    }

    public void testTruncate_Percentage_ReturnsAnEmptyString_WhenPercentageIs0() {
        var truncator = createTruncator(0);
        assertThat(truncator.truncate(List.of("abc")), is(new Truncator.TruncationResult(List.of(""), List.of(true))));
    }

    public void testTruncate_Percentage_ReturnsTheSameValueStringIfPercentageIs1() {
        var truncator = createTruncator(1);
        assertThat(truncator.truncate(List.of("abc")), is(new Truncator.TruncationResult(List.of("abc"), List.of(false))));
    }

    public void testTruncate_Tokens_DoesNotTruncateWhenLimitIsNull() {
        assertThat(
            truncate(List.of("abcd", "123"), null),
            is(new Truncator.TruncationResult(List.of("abcd", "123"), List.of(false, false)))
        );
    }

    public void testTruncate_Tokens_ReducesLengthTo3Characters() {
        assertThat(
            truncate(List.of("abcd", "123 abcd"), 1),
            is(new Truncator.TruncationResult(List.of("abc", "123"), List.of(true, true)))
        );
    }

    public void testTruncate_Tokens_OnlyTruncatesTheFirstEntry() {
        assertThat(truncate(List.of("abcd", "123"), 1), is(new Truncator.TruncationResult(List.of("abc", "123"), List.of(true, false))));
    }

    public void testTruncate_Tokens_ReturnsAnEmptyString_WhenItIsAnEmptyString() {
        assertThat(truncate(List.of(""), 1), is(new Truncator.TruncationResult(List.of(""), List.of(false))));
    }

    public void testTruncate_Tokens_ReturnsAnEmptyString_WhenMaxTokensIs0() {
        assertThat(truncate(List.of("abc"), 0), is(new Truncator.TruncationResult(List.of(""), List.of(true))));
    }

    public void testTruncate_Tokens_ReturnsTheSameValueStringIfTokensIsGreaterThanStringSize() {
        assertThat(truncate(List.of("abc"), 2), is(new Truncator.TruncationResult(List.of("abc"), List.of(false))));
    }

    public void testTruncate_ThrowsIfPercentageIsGreaterThan1() {
        expectThrows(IllegalArgumentException.class, () -> createTruncator(1.001));
    }

    public void testTruncate_ThrowsIfPercentageIsLessThan0() {
        expectThrows(IllegalArgumentException.class, () -> createTruncator(-0.001));
    }

    public static Truncator createTruncator() {
        return new Truncator(Settings.EMPTY, mockClusterServiceEmpty());
    }

    public static Truncator createTruncator(double percentage) {
        var settings = Settings.builder().put(Truncator.REDUCTION_PERCENTAGE_SETTING.getKey(), percentage).build();
        return new Truncator(settings, mockClusterServiceEmpty());
    }
}
