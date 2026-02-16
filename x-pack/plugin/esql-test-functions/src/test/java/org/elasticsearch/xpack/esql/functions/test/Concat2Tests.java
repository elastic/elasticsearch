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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the Concat2 variadic string concatenation function.
 * <p>
 * These tests verify the basic functionality of the Concat2 function class,
 * including type resolution and the process method that will be used
 * for runtime bytecode generation.
 * </p>
 */
public class Concat2Tests extends ESTestCase {

    /**
     * Test the static process method that is used for runtime bytecode generation.
     */
    public void testProcessTwoStrings() {
        BytesRef[] values = { new BytesRef("Hello"), new BytesRef("World") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("HelloWorld"));
    }

    public void testProcessThreeStrings() {
        BytesRef[] values = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("abc"));
    }

    public void testProcessSingleString() {
        BytesRef[] values = { new BytesRef("solo") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("solo"));
    }

    public void testProcessWithSpace() {
        BytesRef[] values = { new BytesRef("Hello"), new BytesRef(" "), new BytesRef("World") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("Hello World"));
    }

    public void testProcessEmptyStrings() {
        BytesRef[] values = { new BytesRef(""), new BytesRef("test") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("test"));
    }

    public void testProcessAllEmpty() {
        BytesRef[] values = { new BytesRef(""), new BytesRef(""), new BytesRef("") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo(""));
    }

    public void testProcessManyStrings() {
        BytesRef[] values = {
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("c"),
            new BytesRef("d"),
            new BytesRef("e") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("abcde"));
    }

    public void testProcessUnicode() {
        BytesRef[] values = { new BytesRef("café"), new BytesRef(" "), new BytesRef("naïve") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("café naïve"));
    }

    public void testProcessSpecialCharacters() {
        BytesRef[] values = { new BytesRef("hello!"), new BytesRef("@#$"), new BytesRef("%^&") };
        assertThat(Concat2.process(values).utf8ToString(), equalTo("hello!@#$%^&"));
    }

    /**
     * Test type resolution for valid string types.
     */
    public void testTypeResolutionKeyword() {
        Concat2 concat2 = new Concat2(
            Source.EMPTY,
            new Literal(Source.EMPTY, new BytesRef("hello"), DataType.KEYWORD),
            List.of(new Literal(Source.EMPTY, new BytesRef("world"), DataType.KEYWORD))
        );
        assertThat(concat2.dataType(), equalTo(DataType.KEYWORD));
        assertThat(concat2.resolveType(), equalTo(Concat2.TypeResolution.TYPE_RESOLVED));
    }

    public void testTypeResolutionText() {
        Concat2 concat2 = new Concat2(
            Source.EMPTY,
            new Literal(Source.EMPTY, new BytesRef("hello"), DataType.TEXT),
            List.of(new Literal(Source.EMPTY, new BytesRef("world"), DataType.TEXT))
        );
        assertThat(concat2.dataType(), equalTo(DataType.KEYWORD));
        assertThat(concat2.resolveType(), equalTo(Concat2.TypeResolution.TYPE_RESOLVED));
    }

    public void testTypeResolutionMixed() {
        Concat2 concat2 = new Concat2(
            Source.EMPTY,
            new Literal(Source.EMPTY, new BytesRef("hello"), DataType.KEYWORD),
            List.of(new Literal(Source.EMPTY, new BytesRef("world"), DataType.TEXT))
        );
        assertThat(concat2.dataType(), equalTo(DataType.KEYWORD));
        assertThat(concat2.resolveType(), equalTo(Concat2.TypeResolution.TYPE_RESOLVED));
    }

    public void testTypeResolutionSingleArg() {
        Concat2 concat2 = new Concat2(
            Source.EMPTY,
            new Literal(Source.EMPTY, new BytesRef("solo"), DataType.KEYWORD),
            List.of()
        );
        assertThat(concat2.dataType(), equalTo(DataType.KEYWORD));
        assertThat(concat2.resolveType(), equalTo(Concat2.TypeResolution.TYPE_RESOLVED));
    }

    /**
     * Test that invalid types are rejected.
     */
    public void testInvalidTypeResolution() {
        Concat2 concat2Int = new Concat2(
            Source.EMPTY,
            new Literal(Source.EMPTY, 42, DataType.INTEGER),
            List.of()
        );
        Concat2.TypeResolution resolution = concat2Int.resolveType();
        assertThat(resolution.resolved(), equalTo(false));
        assertTrue(resolution.message().contains("Expected string type"));
    }

    public void testInvalidTypeResolutionBoolean() {
        Concat2 concat2Bool = new Concat2(
            Source.EMPTY,
            new Literal(Source.EMPTY, true, DataType.BOOLEAN),
            List.of()
        );
        Concat2.TypeResolution resolution = concat2Bool.resolveType();
        assertThat(resolution.resolved(), equalTo(false));
        assertTrue(resolution.message().contains("Expected string type"));
    }

    public void testInvalidTypeInRest() {
        Concat2 concat2 = new Concat2(
            Source.EMPTY,
            new Literal(Source.EMPTY, new BytesRef("hello"), DataType.KEYWORD),
            List.of(new Literal(Source.EMPTY, 42, DataType.INTEGER))
        );
        Concat2.TypeResolution resolution = concat2.resolveType();
        assertThat(resolution.resolved(), equalTo(false));
        assertTrue(resolution.message().contains("Expected string type"));
    }
}
