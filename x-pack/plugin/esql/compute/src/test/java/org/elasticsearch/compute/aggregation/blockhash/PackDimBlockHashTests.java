/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/** Content identity must survive independent dictionaries, projection and operator boundaries. */
public class PackDimBlockHashTests extends ComputeTestCase {
    public void testAcrossPagesAndCompositeKeys() {
        var factory = blockFactory();
        var specs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.PACK_DIM));
        try (
            var hash = BlockHash.build(specs, factory, 32, false);
            var first = packed(factory, "a", "b", "a", "", null);
            var second = packed(factory, "b", "a", "", null);
            var times1 = factory.newConstantLongBlockWith(10, 5);
            var times2 = factory.newConstantLongBlockWith(10, 4)
        ) {
            var a = new Capture();
            var b = new Capture();
            hash.add(new Page(times1, first), a);
            hash.add(new Page(times2, second), b);
            assertEquals(a.ids.get(0), a.ids.get(2));
            assertEquals(List.of(a.ids.get(1), a.ids.get(0), a.ids.get(3), a.ids.get(4)), b.ids);
            assertEquals(4, hash.numKeys());
            try (var selected = hash.nonEmpty()) {
                Block[] keys = hash.getKeys(selected);
                try {
                    assertTrue(keys[1] instanceof PackDimBlock);
                    var replay = new Capture();
                    hash.add(new Page(keys), replay);
                    for (int p = 0; p < selected.getPositionCount(); p++)
                        assertEquals(selected.getInt(p), (int) replay.ids.get(p));
                } finally {
                    Releasables.close(keys);
                }
            }
        }
    }

    public void testLookupDoesNotInsertAndRetainsItsInput() {
        var factory = blockFactory();
        try (
            var build = packed(factory, "a", "b", "");
            var lookup = RowInTableLookup.build(factory, new Block[] { build });
            var probe = packed(factory, "b", "missing", "a", "", null);
            var matches = lookup.lookup(new Page(probe), ByteSizeValue.ofKb(1))
        ) {
            var result = new ArrayList<Integer>();
            while (matches.hasNext()) {
                try (var block = matches.next()) {
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        result.add(block.isNull(p) ? null : block.getInt(block.getFirstValueIndex(p)));
                    }
                }
            }
            assertEquals(java.util.Arrays.asList(1, null, 0, 2, null), result);
        }
    }

    public void testDuplicateBuildRowsAreStillRejected() {
        try (var input = packed(blockFactory(), "a", "a")) {
            expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory(), new Block[] { input }));
        }
    }

    public void testNullEmptyRecordAndPresentNullAreDifferent() {
        var factory = blockFactory();
        try (var builder = factory.newPackDimBlockBuilder(3)) {
            builder.appendNull();
            builder.append(new BytesRef[0], new BytesRef[0]);
            builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("null") });
            try (
                var block = builder.build();
                var hash = BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, ElementType.PACK_DIM)), factory, 32)
            ) {
                var capture = new Capture();
                hash.add(new Page(block), capture);
                assertEquals(List.of(0, 1, 2), capture.ids);
                try (var selected = hash.nonEmpty()) {
                    Block[] keys = hash.getKeys(selected);
                    try {
                        var values = (PackDimBlock) keys[0];
                        assertTrue(values.isNull(0));
                        assertEquals(0, values.getPackDim(values.getFirstValueIndex(1), new PackDimValue()).size());
                        assertTrue(values.getPackDim(values.getFirstValueIndex(2), new PackDimValue()).contains(new BytesRef("a")));
                    } finally {
                        Releasables.close(keys);
                    }
                }
            }
        }
    }

    public void testBreakerCleanup() {
        testWithCrankyBlockFactory(factory -> {
            try (
                var block = packed(factory, "a", "b", "a", "", null);
                var hash = BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, ElementType.PACK_DIM)), factory, 32)
            ) {
                hash.add(new Page(block), new Capture());
                try (var selected = hash.nonEmpty()) {
                    Releasables.close(hash.getKeys(selected));
                }
            }
        });
    }

    private static PackDimBlock packed(BlockFactory factory, String... values) {
        try (var builder = factory.newPackDimBlockBuilder(values.length)) {
            for (String value : values) {
                if (value == null) builder.appendNull();
                else builder.append(new BytesRef[] { new BytesRef("label") }, new BytesRef[] { new BytesRef(value) });
            }
            return builder.build();
        }
    }

    private static class Capture implements GroupingAggregatorFunction.AddInput {
        private final List<Integer> ids = new ArrayList<>();

        private void read(int offset, IntBlock groups) {
            for (int p = 0; p < groups.getPositionCount(); p++) {
                assertEquals(1, groups.getValueCount(p));
                assertEquals(offset + p, ids.size());
                ids.add(groups.getInt(groups.getFirstValueIndex(p)));
            }
        }

        @Override
        public void add(int offset, IntArrayBlock groups) {
            read(offset, groups);
        }

        @Override
        public void add(int offset, IntBigArrayBlock groups) {
            read(offset, groups);
        }

        @Override
        public void add(int offset, IntVector groups) {
            for (int p = 0; p < groups.getPositionCount(); p++) {
                assertEquals(offset + p, ids.size());
                ids.add(groups.getInt(p));
            }
        }

        @Override
        public void close() {}
    }
}
