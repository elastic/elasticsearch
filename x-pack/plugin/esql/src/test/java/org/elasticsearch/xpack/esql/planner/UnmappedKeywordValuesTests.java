/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

/**
 * Unit tests for {@link UnmappedKeywordValues#collect}, covering null, object, scalar, array, and
 * nested-array inputs to verify the keyword projection semantics shared between
 * {@code UnmappedKeywordBlockLoader} and {@code ExpandUnmappedFieldsPostProcessor}.
 */
public class UnmappedKeywordValuesTests extends ESTestCase {

    public void testNullContributesNothing() {
        assertEmpty(null);
    }

    public void testMapContributesNothing() {
        assertEmpty(Map.of("key", "val"));
    }

    public void testEmptyListContributesNothing() {
        assertEmpty(List.of());
    }

    public void testListContainingNullContributesNothing() {
        List<Object> list = new ArrayList<>();
        list.add(null);
        assertEmpty(list);
    }

    public void testListContainingMapContributesNothing() {
        assertEmpty(List.of(Map.of("key", "val")));
    }

    public void testScalarStringBecomesOneElement() {
        assertValues("hello", "hello");
    }

    public void testScalarIntegerStringifiesCorrectly() {
        assertValues(42, "42");
    }

    public void testScalarBooleanStringifiesCorrectly() {
        assertValues(Boolean.TRUE, "true");
    }

    public void testListWithSingleScalarBecomesOneElement() {
        assertValues(List.of("a"), "a");
    }

    public void testListWithMultipleScalarsBecomesMultipleElements() {
        assertValues(List.of("a", "b", "c"), "a", "b", "c");
    }

    public void testListWithMixedScalarAndMapContributesOnlyScalar() {
        List<Object> mixed = new ArrayList<>();
        mixed.add("scalar");
        mixed.add(Map.of("key", "val"));
        assertValues(mixed, "scalar");
    }

    public void testNestedListFlattens() {
        assertValues(List.of(List.of("a", "b"), List.of("c")), "a", "b", "c");
    }

    public void testDeeplyNestedListFlattens() {
        assertValues(List.of(List.of(List.of("deep"))), "deep");
    }

    private static void assertEmpty(Object value) {
        List<BytesRef> out = new ArrayList<>();
        UnmappedKeywordValues.collect(value, out);
        assertThat(out, empty());
    }

    private static void assertValues(Object value, String... expected) {
        List<BytesRef> out = new ArrayList<>();
        UnmappedKeywordValues.collect(value, out);
        List<String> got = out.stream().map(BytesRef::utf8ToString).toList();
        assertThat(got, contains(expected));
    }
}
