/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class InputTextReaderTests extends ComputeTestCase {

    public void testReadSingleValuePositions() throws Exception {
        String[] texts = { "hello", "world", "test" };
        BytesRefBlock block = createSingleValueBlock(texts);

        try (InputTextReader reader = new InputTextReader(block)) {
            assertThat(reader.estimatedSize(), equalTo(texts.length));

            for (int i = 0; i < texts.length; i++) {
                assertThat(reader.readText(i), equalTo(texts[i]));
            }
        }

        allBreakersEmpty();
    }

    public void testReadMultiValuePositions() throws Exception {
        BytesRefBlock block = createMultiValueBlock();

        try (InputTextReader reader = new InputTextReader(block)) {
            assertThat(reader.estimatedSize(), equalTo(2));

            // First position has multiple values that should be concatenated with newlines
            assertThat(reader.readText(0), equalTo("first\nsecond\nthird"));

            // Second position has a single value
            assertThat(reader.readText(1), equalTo("single"));
        }

        allBreakersEmpty();
    }

    public void testReadMultiValuePositionsWithLimit() throws Exception {
        BytesRefBlock block = createMultiValueBlock();

        try (InputTextReader reader = new InputTextReader(block)) {
            // Test limiting to first 2 values out of 3
            assertThat(reader.readText(0, 2), equalTo("first\nsecond"));

            // Test limiting to first 1 value out of 3
            assertThat(reader.readText(0, 1), equalTo("first"));

            // Test limit larger than available values
            assertThat(reader.readText(0, 10), equalTo("first\nsecond\nthird"));

            // Test limit of 0
            assertThat(reader.readText(0, 0), equalTo(""));

            // Test single value position with limit
            assertThat(reader.readText(1, 1), equalTo("single"));
        }

        allBreakersEmpty();
    }

    public void testReadNullValues() throws Exception {
        BytesRefBlock block = createBlockWithNulls();

        try (InputTextReader reader = new InputTextReader(block)) {
            assertThat(reader.estimatedSize(), equalTo(3));

            assertThat(reader.readText(0), equalTo("before"));
            assertThat(reader.readText(1), nullValue());
            assertThat(reader.readText(2), equalTo("after"));
        }

        allBreakersEmpty();
    }

    public void testReadNullValuesWithLimit() throws Exception {
        BytesRefBlock block = createBlockWithNulls();

        try (InputTextReader reader = new InputTextReader(block)) {
            // Null values should return null regardless of limit
            assertThat(reader.readText(0, 1), equalTo("before"));
            assertThat(reader.readText(1, 1), nullValue());
            assertThat(reader.readText(1, 10), nullValue());
            assertThat(reader.readText(2, 1), equalTo("after"));
        }

        allBreakersEmpty();
    }

    public void testReadEmptyStrings() throws Exception {
        String[] texts = { "", "non-empty", "" };
        BytesRefBlock block = createSingleValueBlock(texts);

        try (InputTextReader reader = new InputTextReader(block)) {
            for (int i = 0; i < texts.length; i++) {
                assertThat(reader.readText(i), equalTo(texts[i]));
                assertThat(reader.readText(i, 1), equalTo(texts[i]));
            }
        }

        allBreakersEmpty();
    }

    public void testReadLargeInput() throws Exception {
        int size = between(1000, 5000);
        String[] texts = new String[size];
        for (int i = 0; i < size; i++) {
            texts[i] = "text_" + i + "_" + randomAlphaOfLength(10);
        }

        BytesRefBlock block = createSingleValueBlock(texts);

        try (InputTextReader reader = new InputTextReader(block)) {
            assertThat(reader.estimatedSize(), equalTo(size));

            for (int i = 0; i < size; i++) {
                assertThat(reader.readText(i), equalTo(texts[i]));
                assertThat(reader.readText(i, 1), equalTo(texts[i]));
            }
        }

        allBreakersEmpty();
    }

    public void testReadUnicodeText() throws Exception {
        String[] texts = { "café", "naïve", "résumé", "🚀 rocket", "多语言支持" };
        BytesRefBlock block = createSingleValueBlock(texts);

        try (InputTextReader reader = new InputTextReader(block)) {
            for (int i = 0; i < texts.length; i++) {
                assertThat(reader.readText(i), equalTo(texts[i]));
                assertThat(reader.readText(i, 1), equalTo(texts[i]));
            }
        }

        allBreakersEmpty();
    }

    public void testReadMultipleTimesFromSamePosition() throws Exception {
        String[] texts = { "consistent" };
        BytesRefBlock block = createSingleValueBlock(texts);

        try (InputTextReader reader = new InputTextReader(block)) {
            // Reading the same position multiple times should return the same result
            assertThat(reader.readText(0), equalTo("consistent"));
            assertThat(reader.readText(0), equalTo("consistent"));
            assertThat(reader.readText(0, 1), equalTo("consistent"));
            assertThat(reader.readText(0, 10), equalTo("consistent"));
        }

        allBreakersEmpty();
    }

    public void testLimitBoundaryConditions() throws Exception {
        BytesRefBlock block = createLargeMultiValueBlock();

        try (InputTextReader reader = new InputTextReader(block)) {
            // Test various limit values on a position with 5 values
            assertThat(reader.readText(0, 0), equalTo(""));
            assertThat(reader.readText(0, 1), equalTo("value0"));
            assertThat(reader.readText(0, 2), equalTo("value0\nvalue1"));
            assertThat(reader.readText(0, 3), equalTo("value0\nvalue1\nvalue2"));
            assertThat(reader.readText(0, 4), equalTo("value0\nvalue1\nvalue2\nvalue3"));
            assertThat(reader.readText(0, 5), equalTo("value0\nvalue1\nvalue2\nvalue3\nvalue4"));

            // Test limit beyond available values
            assertThat(reader.readText(0, 10), equalTo("value0\nvalue1\nvalue2\nvalue3\nvalue4"));
            assertThat(reader.readText(0, Integer.MAX_VALUE), equalTo("value0\nvalue1\nvalue2\nvalue3\nvalue4"));
        }

        allBreakersEmpty();
    }

    private BytesRefBlock createSingleValueBlock(String[] texts) {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(texts.length)) {
            for (String text : texts) {
                builder.appendBytesRef(new BytesRef(text));
            }
            return builder.build();
        }
    }

    private BytesRefBlock createMultiValueBlock() {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(2)) {
            // First position: multiple values
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("first"));
            builder.appendBytesRef(new BytesRef("second"));
            builder.appendBytesRef(new BytesRef("third"));
            builder.endPositionEntry();

            // Second position: single value
            builder.appendBytesRef(new BytesRef("single"));

            return builder.build();
        }
    }

    private BytesRefBlock createBlockWithNulls() {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("before"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("after"));
            return builder.build();
        }
    }

    private BytesRefBlock createLargeMultiValueBlock() {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(1)) {
            // Single position with 5 values for testing limits
            builder.beginPositionEntry();
            for (int i = 0; i < 5; i++) {
                builder.appendBytesRef(new BytesRef("value" + i));
            }
            builder.endPositionEntry();

            return builder.build();
        }
    }
}
