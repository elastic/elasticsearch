/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.List;

public class ColumnarNullSourceComparisonTests extends AbstractColumnarNullContractTestCase {

    private static final Scenario KEYWORD = scenario("keyword", () -> "unused");

    public void testNullStringIsNotAMissingField() {
        final List<Object> expected = List.of("null");
        expectThrows(AssertionError.class, () -> assertSourceDropsNulls(KEYWORD, expected, "{}"));
    }

    public void testNullStringIsNotAJsonNull() {
        final List<Object> expected = List.of("null");
        expectThrows(AssertionError.class, () -> assertSourceDropsNulls(KEYWORD, expected, "{\"field\":null}"));
    }

    public void testNullStringIsKept() throws IOException {
        assertSourceDropsNulls(KEYWORD, List.of("null"), "{\"field\":\"null\"}");
    }

    public void testNullStringInAnArrayIsNotAJsonNull() throws IOException {
        final List<Object> expected = List.of("a", "null");
        assertSourceDropsNulls(KEYWORD, expected, "{\"field\":[\"a\",\"null\"]}");
        expectThrows(AssertionError.class, () -> assertSourceDropsNulls(KEYWORD, expected, "{\"field\":[\"a\",null]}"));
    }

    public void testRetainedNullIsRejected() {
        final List<Object> expected = List.of("a");
        expectThrows(AssertionError.class, () -> assertSourceDropsNulls(KEYWORD, expected, "{\"field\":[\"a\",null]}"));
    }

    public void testValuelessFieldMustBeAbsent() throws IOException {
        assertSourceDropsNulls(KEYWORD, List.of(), "{}");
        expectThrows(AssertionError.class, () -> assertSourceDropsNulls(KEYWORD, List.of(), "{\"field\":null}"));
    }

    public void testSequenceIgnoresARetainedNull() throws IOException {
        assertSourceKeepsSequence(KEYWORD, List.of("a", "b"), "{\"field\":[\"a\",null,\"b\"]}");
    }

    public void testSequenceRejectsAMovedValue() {
        final List<Object> expected = List.of("a", "b");
        expectThrows(AssertionError.class, () -> assertSourceKeepsSequence(KEYWORD, expected, "{\"field\":[\"b\",null,\"a\"]}"));
    }

    public void testSequenceRejectsAMissingField() {
        final List<Object> expected = List.of("null");
        expectThrows(AssertionError.class, () -> assertSourceKeepsSequence(KEYWORD, expected, "{}"));
    }
}
