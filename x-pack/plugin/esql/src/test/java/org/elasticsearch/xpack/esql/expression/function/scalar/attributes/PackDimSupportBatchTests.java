/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;

/** Exercises ordinal fast paths independently of expression semantics and without exposing callbacks on blocks. */
public class PackDimSupportBatchTests extends ComputeTestCase {
    public void testOnlyLiveRecordsAreEvaluatedAndExpandedInOrder() {
        var factory = blockFactory();
        try (var builder = factory.newPackDimBlockBuilder(6)) {
            for (String value : new String[] { "a", "unused", "b", "a", "b" }) {
                builder.append(new BytesRef[] { new BytesRef("key") }, new BytesRef[] { new BytesRef(value) });
            }
            builder.appendNull();
            try (
                var input = builder.build();
                var selected = input.filter(true, new int[] { 2, 5, 0, 2 }, 0, 4);
                var batch = new PackDimBatch(selected)
            ) {
                assertEquals(2, batch.records().getPositionCount());
                assertEquals(new BytesRef("b"), batch.records().getPackDim(0, new PackDimValue()).valueAt(0, new BytesRef()));
                try (var expanded = (PackDimBlock) batch.expand(batch.records())) {
                    assertEquals(4, expanded.getPositionCount());
                    assertTrue(expanded.isNull(1));
                    assertEquals(
                        new BytesRef("a"),
                        expanded.getPackDim(expanded.getFirstValueIndex(2), new PackDimValue()).valueAt(0, new BytesRef())
                    );
                }
                try (var values = factory.newIntBlockBuilder(2)) {
                    values.appendInt(10).beginPositionEntry().appendInt(20).appendInt(30).endPositionEntry();
                    try (var result = values.build(); var expanded = batch.expand(result)) {
                        assertEquals(10, BlockUtils.toJavaObject(expanded, 0));
                        assertTrue(expanded.isNull(1));
                        assertEquals(List.of(20, 30), BlockUtils.toJavaObject(expanded, 2));
                        assertEquals(10, BlockUtils.toJavaObject(expanded, 3));
                    }
                }
                try (var strings = factory.newBytesRefBlockBuilder(2)) {
                    strings.appendBytesRef(new BytesRef("B")).appendBytesRef(new BytesRef("A"));
                    try (var result = strings.build(); var expanded = batch.expand(result)) {
                        assertEquals(new BytesRef("B"), BlockUtils.toJavaObject(expanded, 0));
                        assertEquals(new BytesRef("A"), BlockUtils.toJavaObject(expanded, 2));
                        assertTrue(expanded.isNull(1));
                    }
                }
            }
        }
    }

    public void testEmptyAndNullInputsDoNotExposeDictionaryMembers() {
        for (int count : new int[] { 0, 3 }) {
            try (
                var input = (PackDimBlock) blockFactory().newConstantNullBlock(count);
                var batch = new PackDimBatch(input);
                var output = batch.expand(batch.records())
            ) {
                assertEquals(0, batch.records().getPositionCount());
                assertEquals(count, output.getPositionCount());
                for (int p = 0; p < count; p++)
                    assertTrue(output.isNull(p));
            }
        }
    }

    public void testWrongRecordCountRejected() {
        try (
            var input = (PackDimBlock) blockFactory().newConstantNullBlock(3);
            var batch = new PackDimBatch(input);
            var wrong = blockFactory().newConstantNullBlock(1)
        ) {
            expectThrows(IllegalArgumentException.class, () -> batch.expand(wrong));
        }
    }

    public void testBreakerFailureReleasesBatchAndResult() {
        testWithCrankyBlockFactory(factory -> {
            try (var builder = factory.newPackDimBlockBuilder(5)) {
                for (int p = 0; p < 5; p++) {
                    if (p == 2) builder.appendNull();
                    else builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("value") });
                }
                try (var input = builder.build(); var batch = new PackDimBatch(input); Block output = batch.expand(batch.records())) {
                    assertEquals(5, output.getPositionCount());
                    assertTrue(output.isNull(2));
                }
            }
        });
    }
}
