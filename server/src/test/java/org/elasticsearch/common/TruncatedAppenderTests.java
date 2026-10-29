/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TruncatedAppenderTests extends ESTestCase {

    /** Values that fit are rendered exactly as their {@code toString()} renders them. */
    public void testValuesThatFitMatchToString() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("string", "value");
        map.put("number", 3);
        map.put("flag", true);
        map.put("nothing", null);
        map.put(null, "null key");
        map.put("list", List.of("a", 1, Map.of("k", "v")));
        map.put("set", Set.of("only"));
        map.put("builder", new StringBuilder("chars"));
        map.put("nested", Map.of("empty", List.of()));

        for (Object value : Arrays.asList(map, Map.entry("key", List.of("value")), List.of(1, List.of()), "text", 42, null)) {
            String expected = String.valueOf(value);
            assertThat(appended(value, expected.length() + between(0, 10), true), equalTo(expected));
        }
    }

    /** A value that is iterable but not a collection, such as a path, is rendered with its own {@code toString()}. */
    public void testRendersNonCollectionIterableWithToString() {
        Path path = PathUtils.get("a", "b");

        assertThat(appended(path, 100, true), equalTo(path.toString()));
    }

    /** Large nested values, such as whole documents keyed by id, are cut at the limit. */
    public void testCutsLargeNestedValues() {
        int maxLength = between(1, 1024);
        Map<String, Object> docs = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            docs.put("doc-" + i, Map.of("message", "x".repeat(1024), "tags", List.of("a", "b")));
        }

        assertThat(appended(docs, maxLength, false), equalTo(docs.toString().substring(0, maxLength)));
    }

    /** Many small values that add up past the limit are cut wherever the limit falls, including inside a key, value or separator. */
    public void testCutsManySmallValues() {
        int maxLength = between(1, 1024);
        Map<String, Object> params = new LinkedHashMap<>();
        for (int i = 0; i < 100; i++) {
            params.put(randomAlphaOfLength(between(1, 10)) + i, List.of(i, "value" + i));
        }

        assertThat(appended(params, maxLength, false), equalTo(params.toString().substring(0, maxLength)));
    }

    /** A large character sequence is copied only up to the limit. */
    public void testCutsLargeCharSequence() {
        int maxLength = between(0, 1024);

        assertThat(appended(new StringBuilder("x".repeat(maxLength * 10 + 1)), maxLength, false), equalTo("x".repeat(maxLength)));
    }

    /** Any other value is rendered with {@code toString()} and then cut. */
    public void testCutsLongToStringOfOtherValues() {
        int maxLength = between(1, 1024);
        BigInteger value = BigInteger.TEN.pow(maxLength);

        assertThat(appended(value, maxLength, false), equalTo(value.toString().substring(0, maxLength)));
    }

    /** The limit counts the brackets, so a value is complete only if its closing bracket fits too. */
    public void testLimitCountsClosingBracket() {
        Map<String, Object> value = Map.of("message", randomAlphaOfLength(between(1, 100)));
        String full = value.toString();

        assertThat(appended(value, full.length(), true), equalTo(full));
        assertThat(appended(value, full.length() - 1, false), equalTo(full.substring(0, full.length() - 1)));
    }

    /** A cut does not leave the first half of a surrogate pair at the end. */
    public void testDoesNotSplitSurrogatePairs() {
        int maxLength = between(1, 1024);
        // the high surrogate of the emoji would be the last character that fits
        String value = "x".repeat(maxLength - 1) + Character.toString(0x1F600) + "tail";

        assertThat(appended(value, maxLength, false), equalTo("x".repeat(maxLength - 1)));
    }

    /** A surrogate pair that ends exactly at the limit is kept whole. */
    public void testKeepsSurrogatePairThatFits() {
        int maxLength = between(2, 1024);
        String emoji = Character.toString(0x1F600);
        String value = "x".repeat(maxLength - 2) + emoji + "tail";

        assertThat(appended(value, maxLength, false), equalTo("x".repeat(maxLength - 2) + emoji));
    }

    /** With no room, nothing is appended, and only an empty value counts as complete. */
    public void testZeroMaxLength() {
        assertThat(appended("", 0, true), equalTo(""));
        assertThat(appended("x", 0, false), equalTo(""));
        assertThat(appended(Map.of(), 0, false), equalTo(""));
        assertThat(appended(List.of(), 0, false), equalTo(""));
        assertThat(appended(null, 0, false), equalTo(""));
    }

    /** The limit counts from the end of what is already in the builder. */
    public void testLimitStartsAtTheEndOfTheBuilder() {
        StringBuilder b = new StringBuilder("prefix ");

        assertThat(TruncatedAppender.append(b, "x".repeat(20), 10), equalTo(false));
        assertThat(b.toString(), equalTo("prefix " + "x".repeat(10)));
    }

    /** A limit that would run past the largest builder length is treated as no limit. */
    public void testMaxLengthDoesNotOverflow() {
        StringBuilder b = new StringBuilder("prefix");

        assertThat(TruncatedAppender.append(b, "x", Integer.MAX_VALUE), equalTo(true));
        assertThat(b.toString(), equalTo("prefixx"));
    }

    /** A negative limit is a caller bug, so it is rejected instead of being treated as zero. */
    public void testRejectsNegativeMaxLength() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TruncatedAppender.append(new StringBuilder(), "x", -1)
        );
        assertThat(e.getMessage(), equalTo("maxLength must not be negative but was [-1]"));
    }

    private static String appended(Object value, int maxLength, boolean expectComplete) {
        StringBuilder b = new StringBuilder();
        assertThat(TruncatedAppender.append(b, value, maxLength), equalTo(expectComplete));
        return b.toString();
    }
}
