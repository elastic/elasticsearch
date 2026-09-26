/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.mapper.BlockLoader;

public class PackDimBlockTests extends ComputeTestCase {
    public void testReaderAndComputeUseTheSameBuilder() {
        var factory = blockFactory();
        BlockLoader.BlockFactory loaderFactory = new DelegatingBlockLoaderFactory(factory) {
            @Override
            public Block constantNulls(int count) {
                return factory.newConstantNullBlock(count);
            }
        };
        BytesRef[] names = { new BytesRef("dimension") };
        BytesRef[] values = { new BytesRef("null") };
        try (var readerBuilder = loaderFactory.packDimBlockBuilder(1)) {
            readerBuilder.append(names, values);
            values[0].bytes[values[0].offset] = 'x';
            try (var output = (PackDimBlock) readerBuilder.build()) {
                assertEquals(new BytesRef("null"), output.getPackDim(0, new PackDimValue()).get(names[0], new BytesRef()));
            }
        }
    }

    public void testSelectionEmptyAndNull() {
        BlockFactory factory = blockFactory();
        try (var builder = factory.newPackDimBlockBuilder(4)) {
            builder.append(new BytesRef[] { new BytesRef("region") }, new BytesRef[] { new BytesRef("\"eu\"") });
            builder.append(new BytesRef[] { new BytesRef("region") }, new BytesRef[] { new BytesRef("\"eu\"") });
            builder.append(new BytesRef[0], new BytesRef[0]);
            builder.appendNull();
            try (var block = builder.build(); var filtered = block.filter(true, new int[] { 1, 0, 1, 3, 2 }, 0, 5)) {
                assertEquals(
                    new BytesRef("\"eu\""),
                    filtered.getPackDim(filtered.getFirstValueIndex(2), new PackDimValue()).get(new BytesRef("region"), new BytesRef())
                );
                assertTrue(filtered.isNull(3));
                assertFalse(filtered.isNull(4));
                assertEquals(0, filtered.getPackDim(filtered.getFirstValueIndex(4), new PackDimValue()).size());
                assertNull(block.getPackDim(block.getFirstValueIndex(0), new PackDimValue()).get(new BytesRef("absent"), new BytesRef()));
                assertEquals(
                    new BytesRef("\"eu\""),
                    block.getPackDim(block.getFirstValueIndex(0), new PackDimValue()).get(new BytesRef("region"), new BytesRef())
                );
            }
        }
    }

    public void testRoundTrip() throws Exception {
        BlockFactory factory = blockFactory();
        try (var builder = factory.newPackDimBlockBuilder(3); var out = new BytesStreamOutput()) {
            builder.append(
                new BytesRef[] { new BytesRef("a"), new BytesRef("b") },
                new BytesRef[] { new BytesRef("1"), new BytesRef("[2,3]") }
            );
            builder.append(new BytesRef[] { new BytesRef("region") }, new BytesRef[] { new BytesRef("\"eu\"") });
            builder.appendNull();
            try (var original = builder.build()) {
                out.setTransportVersion(TransportVersion.current());
                Block.writeTypedBlock(original, out);
                try (var in = new BlockStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), factory)) {
                    in.setTransportVersion(TransportVersion.current());
                    try (var read = (PackDimBlock) Block.readTypedBlock(in)) {
                        assertEquals(original.getPositionCount(), read.getPositionCount());
                        assertRecordsEqual(original, read);
                    }
                }
            }
        }
    }

    public void testRejectAmbiguousNames() {
        try (var builder = blockFactory().newPackDimBlockBuilder(1)) {
            expectThrows(
                IllegalArgumentException.class,
                () -> builder.append(
                    new BytesRef[] { new BytesRef("x"), new BytesRef("x") },
                    new BytesRef[] { new BytesRef("1"), new BytesRef("2") }
                )
            );
        }
    }

    public void testLookupAndDeepCopyPreserveContent() {
        var factory = blockFactory();
        try (var builder = factory.newPackDimBlockBuilder(3); var positions = factory.newIntBlockBuilder(4)) {
            builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("v") });
            builder.append(new BytesRef[] { new BytesRef("region") }, new BytesRef[] { new BytesRef("\"eu\"") });
            builder.appendNull();
            positions.appendInt(1).appendInt(0).appendInt(2).appendInt(10);
            try (
                var input = builder.build();
                var copy = input.deepCopy(factory);
                var selection = positions.build();
                var lookup = input.lookup(selection, ByteSizeValue.ofKb(1))
            ) {
                assertRecordsEqual(input, copy);
                try (var result = (PackDimBlock) lookup.next()) {
                    assertEquals(4, result.getPositionCount());
                    assertNotNull(result.getPackDim(result.getFirstValueIndex(0), new PackDimValue()));
                    assertTrue(result.isNull(2));
                    assertTrue(result.isNull(3));
                }
                assertFalse(lookup.hasNext());
            }
        }
    }

    public void testOldRecipientRejected() throws Exception {
        try (var builder = blockFactory().newPackDimBlockBuilder(1); var out = new BytesStreamOutput()) {
            builder.append(new BytesRef[0], new BytesRef[0]);
            try (var block = builder.build()) {
                out.setTransportVersion(TransportVersion.minimumCompatible());
                expectThrows(java.io.IOException.class, () -> Block.writeTypedBlock(block, out));
            }
        }
    }

    public void testBreakerFailureReleasesChildren() {
        testWithCrankyBlockFactory(factory -> {
            try (var builder = factory.newPackDimBlockBuilder(10)) {
                for (int p = 0; p < 10; p++)
                    builder.append(new BytesRef[] { new BytesRef("key") }, new BytesRef[] { new BytesRef("value") });
                try (var block = builder.build(); var filtered = block.slice(2, 5); var copy = filtered.deepCopy(factory)) {
                    assertEquals(3, copy.getPositionCount());
                }
            }
        });
    }

    public void testRepeatedRecordsAreInternedByContent() {
        try (var builder = blockFactory().newPackDimBlockBuilder(4)) {
            builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("x") });
            builder.appendNull();
            builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("x") });
            builder.append(new BytesRef[0], new BytesRef[0]);
            try (var input = builder.build(); var copy = blockFactory().newPackDimBlockBuilder(4)) {
                assertEquals(2, input.asOrdinalPackDim().getDictionarySize());
                PackDimValue scratch = new PackDimValue();
                assertSame(scratch, input.getPackDim(input.getFirstValueIndex(2), scratch));
                assertTrue(scratch.contains(new BytesRef("a")));
                assertFalse(scratch.contains(new BytesRef("missing")));
                copy.copyFrom(input, 0, 4);
                try (var result = copy.build()) {
                    assertRecordsEqual(input, result);
                }
                input.getPackDim(input.getFirstValueIndex(3), scratch);
                assertEquals(0, scratch.size());
            }
        }
    }

    public void testMaskAndSliceShareDictionaryWithoutChangingInput() {
        var factory = blockFactory();
        try (var builder = factory.newPackDimBlockBuilder(3); var maskBuilder = factory.newBooleanVectorFixedBuilder(3)) {
            for (String v : new String[] { "one", "two", "three" }) {
                builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef(v) });
            }
            maskBuilder.appendBoolean(0, true).appendBoolean(1, false).appendBoolean(2, true);
            try (
                var input = builder.build();
                var mask = maskBuilder.build();
                var masked = input.keepMask(mask);
                var slice = masked.slice(1, 3)
            ) {
                assertTrue(slice.isNull(0));
                assertFalse(input.isNull(1));
                assertEquals(
                    new BytesRef("three"),
                    slice.getPackDim(slice.getFirstValueIndex(1), new PackDimValue()).valueAt(0, new BytesRef())
                );
                assertEquals(3, slice.asOrdinalPackDim().getDictionarySize());
            }
        }
    }

    public void testEmptyAndAllNullBlocks() {
        for (int count : new int[] { 0, 3 }) {
            try (var builder = blockFactory().newPackDimBlockBuilder(count)) {
                for (int p = 0; p < count; p++)
                    builder.appendNull();
                try (var input = builder.build(); var filtered = input.slice(0, count)) {
                    assertEquals(count, filtered.getPositionCount());
                    for (int p = 0; p < count; p++)
                        assertTrue(filtered.isNull(p));
                }
            }
        }
        try (PackDimBlock block = (PackDimBlock) blockFactory().newConstantNullBlock(2)) {
            assertNull(block.asOrdinalPackDim());
            assertTrue(block.areAllValuesNull());
        }
    }

    public void testCopyFromConstantNullAndEmptyRecords() {
        try (var nulls = blockFactory().newConstantNullBlock(2); var builder = blockFactory().newPackDimBlockBuilder(3)) {
            builder.copyFrom(nulls, 0, 2);
            builder.append(new BytesRef[0], new BytesRef[0]);
            try (var result = builder.build()) {
                assertTrue(result.isNull(0));
                assertTrue(result.isNull(1));
                assertFalse(result.isNull(2));
                assertEquals(0, result.getPackDim(result.getFirstValueIndex(2), new PackDimValue()).size());
            }
        }
    }

    private static void assertRecordsEqual(PackDimBlock first, PackDimBlock second) {
        assertEquals(first.getPositionCount(), second.getPositionCount());
        for (int p = 0; p < first.getPositionCount(); p++) {
            assertEquals(first.isNull(p), second.isNull(p));
            if (first.isNull(p)) continue;
            var left = first.getPackDim(first.getFirstValueIndex(p), new PackDimValue());
            var right = second.getPackDim(second.getFirstValueIndex(p), new PackDimValue());
            assertEquals(left.size(), right.size());
            for (int i = 0; i < left.size(); i++) {
                assertEquals(left.nameAt(i, new BytesRef()), right.nameAt(i, new BytesRef()));
                assertEquals(left.valueAt(i, new BytesRef()), right.valueAt(i, new BytesRef()));
            }
        }
    }
}
