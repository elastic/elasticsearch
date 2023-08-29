/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BlockValueAsserter {

    static void assertBlockValues(Block block, List<List<Object>> expectedBlockValues) {
        assertThat(block.getPositionCount(), is(equalTo(expectedBlockValues.size())));
        for (int pos = 0; pos < expectedBlockValues.size(); pos++) {
            List<Object> expectedRowValues = expectedBlockValues.get(pos);
            if (expectedRowValues == null || expectedRowValues.isEmpty()) { // TODO empty is not the same as null
                assertThat(block.isNull(pos), is(equalTo(true)));
                assertThat(block.getValueCount(pos), is(equalTo(0)));
            } else {
                assertThat(block.isNull(pos), is(equalTo(false)));
                final int valueCount = block.getValueCount(pos);
                assertThat(expectedRowValues.size(), is(equalTo(valueCount)));
                final int firstValueIndex = block.getFirstValueIndex(pos);
                switch (block.elementType()) {
                    case INT -> assertIntRowValues((IntBlock) block, firstValueIndex, valueCount, expectedRowValues);
                    case LONG -> assertLongRowValues((LongBlock) block, firstValueIndex, valueCount, expectedRowValues);
                    case DOUBLE -> assertDoubleRowValues((DoubleBlock) block, firstValueIndex, valueCount, expectedRowValues);
                    case BYTES_REF -> assertBytesRefRowValues((BytesRefBlock) block, firstValueIndex, valueCount, expectedRowValues);
                    case BOOLEAN -> assertBooleanRowValues((BooleanBlock) block, firstValueIndex, valueCount, expectedRowValues);
                    default -> throw new IllegalArgumentException("Unsupported element type [" + block.elementType() + "]");
                }
            }
        }
    }

    private static void assertIntRowValues(IntBlock block, int firstValueIndex, int valueCount, List<Object> expectedRowValues) {
        for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            int expectedValue = ((Number) expectedRowValues.get(valueIndex)).intValue();
            assertThat(block.getInt(firstValueIndex + valueIndex), is(equalTo(expectedValue)));
        }
    }

    private static void assertLongRowValues(LongBlock block, int firstValueIndex, int valueCount, List<Object> expectedRowValues) {
        for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            long expectedValue = ((Number) expectedRowValues.get(valueIndex)).longValue();
            assertThat(block.getLong(firstValueIndex + valueIndex), is(equalTo(expectedValue)));
        }
    }

    private static void assertDoubleRowValues(DoubleBlock block, int firstValueIndex, int valueCount, List<Object> expectedRowValues) {
        for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            double expectedValue = ((Number) expectedRowValues.get(valueIndex)).doubleValue();
            assertThat(block.getDouble(firstValueIndex + valueIndex), is(equalTo(expectedValue)));
        }
    }

    private static void assertBytesRefRowValues(BytesRefBlock block, int firstValueIndex, int valueCount, List<Object> expectedRowValues) {
        for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            Object value = expectedRowValues.get(valueIndex);
            BytesRef expectedValue;
            if (value instanceof BytesRef b) {
                expectedValue = b;
            } else {
                expectedValue = new BytesRef(expectedRowValues.get(valueIndex).toString());
            }
            assertThat(block.getBytesRef(firstValueIndex + valueIndex, new BytesRef()), is(equalTo(expectedValue)));
        }
    }

    private static void assertBooleanRowValues(BooleanBlock block, int firstValueIndex, int valueCount, List<Object> expectedRowValues) {
        for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            boolean expectedValue = (Boolean) expectedRowValues.get(valueIndex);
            assertThat(block.getBoolean(firstValueIndex + valueIndex), is(equalTo(expectedValue)));
        }
    }
}
