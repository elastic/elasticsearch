/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the Upper2 function.
 * <p>
 * These tests verify the basic functionality of the Upper2 function class,
 * including type resolution and the process method that will be used
 * for runtime bytecode generation.
 * </p>
 */
public class Upper2Tests extends ESTestCase {

    /**
     * Test the static process method that is used for runtime bytecode generation.
     */
    public void testProcessBasic() {
        assertThat(Upper2.process(new BytesRef("hello")).utf8ToString(), equalTo("HELLO"));
        assertThat(Upper2.process(new BytesRef("HELLO")).utf8ToString(), equalTo("HELLO"));
        assertThat(Upper2.process(new BytesRef("Hello World")).utf8ToString(), equalTo("HELLO WORLD"));
    }

    public void testProcessEmpty() {
        assertThat(Upper2.process(new BytesRef("")).utf8ToString(), equalTo(""));
    }

    public void testProcessNull() {
        assertThat(Upper2.process(null), nullValue());
    }

    public void testProcessMixedCase() {
        assertThat(Upper2.process(new BytesRef("HeLLo WoRLd")).utf8ToString(), equalTo("HELLO WORLD"));
        assertThat(Upper2.process(new BytesRef("abc123DEF")).utf8ToString(), equalTo("ABC123DEF"));
    }

    public void testProcessSpecialCharacters() {
        assertThat(Upper2.process(new BytesRef("hello!@#$%")).utf8ToString(), equalTo("HELLO!@#$%"));
        assertThat(Upper2.process(new BytesRef("test_value")).utf8ToString(), equalTo("TEST_VALUE"));
        assertThat(Upper2.process(new BytesRef("test-value")).utf8ToString(), equalTo("TEST-VALUE"));
    }

    public void testProcessUnicode() {
        // Test with accented characters
        assertThat(Upper2.process(new BytesRef("café")).utf8ToString(), equalTo("CAFÉ"));
        assertThat(Upper2.process(new BytesRef("naïve")).utf8ToString(), equalTo("NAÏVE"));
    }

    public void testProcessNumbers() {
        // Numbers should remain unchanged
        assertThat(Upper2.process(new BytesRef("123")).utf8ToString(), equalTo("123"));
        assertThat(Upper2.process(new BytesRef("test123")).utf8ToString(), equalTo("TEST123"));
    }

    /**
     * Test type resolution for valid string types.
     */
    public void testTypeResolutionKeyword() {
        Upper2 upper2 = new Upper2(Source.EMPTY, new Literal(Source.EMPTY, new BytesRef("hello"), DataType.KEYWORD));
        assertThat(upper2.dataType(), equalTo(DataType.KEYWORD));
        assertThat(upper2.resolveType(), equalTo(Upper2.TypeResolution.TYPE_RESOLVED));
    }

    public void testTypeResolutionText() {
        Upper2 upper2 = new Upper2(Source.EMPTY, new Literal(Source.EMPTY, new BytesRef("hello"), DataType.TEXT));
        assertThat(upper2.dataType(), equalTo(DataType.KEYWORD)); // Always returns keyword
        assertThat(upper2.resolveType(), equalTo(Upper2.TypeResolution.TYPE_RESOLVED));
    }

    /**
     * Test that invalid types are rejected.
     */
    public void testInvalidTypeResolution() {
        // Test with integer (invalid for upper2)
        Upper2 upper2Int = new Upper2(Source.EMPTY, new Literal(Source.EMPTY, 42, DataType.INTEGER));
        Upper2.TypeResolution resolution = upper2Int.resolveType();
        assertThat(resolution.resolved(), equalTo(false));
        assertTrue(resolution.message().contains("Expected string type"));
    }

    public void testInvalidTypeResolutionBoolean() {
        Upper2 upper2Bool = new Upper2(Source.EMPTY, new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        Upper2.TypeResolution resolution = upper2Bool.resolveType();
        assertThat(resolution.resolved(), equalTo(false));
        assertTrue(resolution.message().contains("Expected string type"));
    }

    /**
     * Test edge cases.
     */
    public void testEdgeCases() {
        // Single character
        assertThat(Upper2.process(new BytesRef("a")).utf8ToString(), equalTo("A"));
        assertThat(Upper2.process(new BytesRef("Z")).utf8ToString(), equalTo("Z"));

        // Whitespace only
        assertThat(Upper2.process(new BytesRef("   ")).utf8ToString(), equalTo("   "));

        // Newlines and tabs
        assertThat(Upper2.process(new BytesRef("hello\nworld")).utf8ToString(), equalTo("HELLO\nWORLD"));
        assertThat(Upper2.process(new BytesRef("hello\tworld")).utf8ToString(), equalTo("HELLO\tWORLD"));
    }
}
