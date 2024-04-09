/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EnrichResultBuilderTests extends ESTestCase {

    public void testBytesRef() {
        BlockFactory blockFactory = blockFactory();
        Map<Integer, List<BytesRef>> expectedValues = new HashMap<>();
        int numPages = between(0, 10);
        int maxPosition = between(0, 100);
        var resultBuilder = EnrichResultBuilder.enrichResultBuilder(ElementType.BYTES_REF, blockFactory, 0, maxPosition + 1);
        for (int i = 0; i < numPages; i++) {
            int numRows = between(1, 100);
            try (
                var positionsBuilder = blockFactory.newIntVectorBuilder(numRows);
                var valuesBuilder = blockFactory.newBytesRefBlockBuilder(numRows)
            ) {
                for (int r = 0; r < numRows; r++) {
                    int position = between(0, maxPosition);
                    positionsBuilder.appendInt(position);
                    int numValues = between(0, 3);
                    if (numValues == 0) {
                        valuesBuilder.appendNull();
                    }
                    if (numValues > 1) {
                        valuesBuilder.beginPositionEntry();
                    }
                    for (int v = 0; v < numValues; v++) {
                        BytesRef val = new BytesRef(randomByteArrayOfLength(10));
                        expectedValues.computeIfAbsent(position, k -> new ArrayList<>()).add(val);
                        valuesBuilder.appendBytesRef(val);
                    }
                    if (numValues > 1) {
                        valuesBuilder.endPositionEntry();
                    }
                }
                try (var positions = positionsBuilder.build(); var valuesBlock = valuesBuilder.build()) {
                    resultBuilder.addInputPage(positions, new Page(valuesBlock));
                }
            }
        }
        try (BytesRefBlock actualOutput = (BytesRefBlock) resultBuilder.build()) {
            assertThat(actualOutput.getPositionCount(), equalTo(maxPosition + 1));
            for (int i = 0; i < actualOutput.getPositionCount(); i++) {
                List<BytesRef> values = expectedValues.get(i);
                if (actualOutput.isNull(i)) {
                    assertNull(values);
                } else {
                    int valueCount = actualOutput.getValueCount(i);
                    int first = actualOutput.getFirstValueIndex(i);
                    assertThat(valueCount, equalTo(values.size()));
                    for (int v = 0; v < valueCount; v++) {
                        assertThat(actualOutput.getBytesRef(first + v, new BytesRef()), equalTo(values.get(v)));
                    }
                }
            }
        }
        resultBuilder.close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testLong() {
        BlockFactory blockFactory = blockFactory();
        Map<Integer, List<Long>> expectedValues = new HashMap<>();
        int numPages = between(0, 10);
        int maxPosition = between(0, 100);
        var resultBuilder = EnrichResultBuilder.enrichResultBuilder(ElementType.LONG, blockFactory, 0, maxPosition + 1);
        for (int i = 0; i < numPages; i++) {
            int numRows = between(1, 100);
            try (
                var positionsBuilder = blockFactory.newIntVectorBuilder(numRows);
                var valuesBuilder = blockFactory.newLongBlockBuilder(numRows)
            ) {
                for (int r = 0; r < numRows; r++) {
                    int position = between(0, maxPosition);
                    positionsBuilder.appendInt(position);
                    int numValues = between(0, 3);
                    if (numValues == 0) {
                        valuesBuilder.appendNull();
                    }
                    if (numValues > 1) {
                        valuesBuilder.beginPositionEntry();
                    }
                    for (int v = 0; v < numValues; v++) {
                        long val = randomLong();
                        expectedValues.computeIfAbsent(position, k -> new ArrayList<>()).add(val);
                        valuesBuilder.appendLong(val);
                    }
                    if (numValues > 1) {
                        valuesBuilder.endPositionEntry();
                    }
                }
                try (var positions = positionsBuilder.build(); var valuesBlock = valuesBuilder.build()) {
                    resultBuilder.addInputPage(positions, new Page(valuesBlock));
                }
            }
        }
        try (LongBlock actualOutput = (LongBlock) resultBuilder.build()) {
            assertThat(actualOutput.getPositionCount(), equalTo(maxPosition + 1));
            for (int i = 0; i < actualOutput.getPositionCount(); i++) {
                List<Long> values = expectedValues.get(i);
                if (actualOutput.isNull(i)) {
                    assertNull(values);
                } else {
                    int valueCount = actualOutput.getValueCount(i);
                    int first = actualOutput.getFirstValueIndex(i);
                    assertThat(valueCount, equalTo(values.size()));
                    for (int v = 0; v < valueCount; v++) {
                        assertThat(actualOutput.getLong(first + v), equalTo(values.get(v)));
                    }
                }
            }
        }
        resultBuilder.close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    BlockFactory blockFactory() {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(100)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        return new BlockFactory(breaker, bigArrays);
    }
}
