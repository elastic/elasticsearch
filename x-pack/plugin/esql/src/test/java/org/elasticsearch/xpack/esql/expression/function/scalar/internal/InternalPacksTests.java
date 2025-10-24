/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.internal;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class InternalPacksTests extends ComputeTestCase {

    private static List<BytesRef> randomBytesRefs(int count) {
        List<BytesRef> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(new BytesRef(ESTestCase.randomByteArrayOfLength(between(1, 100))));
        }
        return values;
    }

    private static Block buildBlock(BlockFactory blockFactory, ElementType elementType, List<List<?>> rows) {
        try (Block.Builder builder = elementType.newBlockBuilder(rows.size(), blockFactory)) {
            for (List<?> row : rows) {
                if (row.isEmpty()) {
                    builder.appendNull();
                    continue;
                }
                if (row.size() > 1) {
                    builder.beginPositionEntry();
                }
                for (Object o : row) {
                    BlockUtils.appendValue(builder, o, elementType);
                }
                if (row.size() > 1) {
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static boolean encodedRowEquals(BytesRefBlock b1, BytesRefBlock b2, int position) {
        int count1 = b1.getValueCount(position);
        int count2 = b2.getValueCount(position);
        if (count1 == 0 && count2 == 0) {
            return true;
        }
        if (count1 != count2) {
            return false;
        }
        assertThat(count1, equalTo(1));
        assertThat(count2, equalTo(1));
        BytesRef v1 = b1.getBytesRef(b1.getFirstValueIndex(position), new BytesRef());
        BytesRef v2 = b2.getBytesRef(b2.getFirstValueIndex(position), new BytesRef());
        return v1.equals(v2);
    }

    public void testKeyword() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
        int positionCount = between(1, 100);
        List<List<?>> values1 = new ArrayList<>();
        List<List<?>> values2 = new ArrayList<>();
        for (int i = 0; i < positionCount; i++) {
            List<BytesRef> v = randomBytesRefs(between(0, 5));
            values1.add(v);
            if (randomBoolean()) {
                values2.add(v);
            } else {
                values2.add(randomValueOtherThan(v, () -> randomBytesRefs(between(0, 5))));
            }
        }
        var block1 = (BytesRefBlock) buildBlock(blockFactory, ElementType.BYTES_REF, values1);
        var block2 = (BytesRefBlock) buildBlock(blockFactory, ElementType.BYTES_REF, values2);
        var encoded1 = InternalPacks.packBytesValues(driverContext, block1);
        var encoded2 = InternalPacks.packBytesValues(driverContext, block2);
        var decoded1 = InternalPacks.unpackBytesValues(driverContext, encoded1);
        var decoded2 = InternalPacks.unpackBytesValues(driverContext, encoded2);
        try {
            assertThat(decoded1, equalTo(block1));
            assertThat(decoded2, equalTo(block2));
            for (int p = 0; p < positionCount; p++) {
                List<?> v1 = values1.get(p);
                List<?> v2 = values2.get(p);
                boolean equals = encodedRowEquals(encoded1, encoded2, p);
                if (v1.equals(v2)) {
                    assertTrue(equals);
                } else {
                    assertFalse(equals);
                }
            }
        } finally {
            Releasables.close(block1, encoded1, decoded1, block2, encoded2, decoded2);
        }
    }

    public void testLongs() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
        int positionCount = between(1, 100);
        List<List<?>> values1 = new ArrayList<>();
        List<List<?>> values2 = new ArrayList<>();
        for (int i = 0; i < positionCount; i++) {
            List<Long> v = randomLongs(between(0, 5)).boxed().toList();
            values1.add(v);
            if (randomBoolean()) {
                values2.add(v);
            } else {
                values2.add(randomValueOtherThan(v, () -> randomLongs(between(0, 5)).boxed().toList()));
            }
        }
        var block1 = (LongBlock) buildBlock(blockFactory, ElementType.LONG, values1);
        var block2 = (LongBlock) buildBlock(blockFactory, ElementType.LONG, values2);
        var encode1 = InternalPacks.packLongValues(driverContext, block1);
        var encode2 = InternalPacks.packLongValues(driverContext, block2);
        var decoded1 = InternalPacks.unpackLongValues(driverContext, encode1);
        var decoded2 = InternalPacks.unpackLongValues(driverContext, encode2);
        try {
            assertThat(decoded1, equalTo(block1));
            assertThat(decoded2, equalTo(block2));
            for (int p = 0; p < positionCount; p++) {
                List<?> v1 = values1.get(p);
                List<?> v2 = values2.get(p);
                if (v1.equals(v2)) {
                    assertTrue(encodedRowEquals(encode1, encode2, p));
                } else {
                    assertFalse(encodedRowEquals(encode1, encode2, p));
                }
            }
        } finally {
            Releasables.close(block1, encode1, decoded1, block2, encode2, decoded2);
        }
    }

    public void testInts() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
        int positionCount = between(1, 100);
        List<List<?>> values1 = new ArrayList<>();
        List<List<?>> values2 = new ArrayList<>();
        for (int i = 0; i < positionCount; i++) {
            List<Integer> v = randomInts(between(0, 5)).boxed().toList();
            values1.add(v);
            if (randomBoolean()) {
                values2.add(v);
            } else {
                values2.add(randomValueOtherThan(v, () -> randomInts(between(0, 5)).boxed().toList()));
            }
        }
        var block1 = (IntBlock) buildBlock(blockFactory, ElementType.INT, values1);
        var block2 = (IntBlock) buildBlock(blockFactory, ElementType.INT, values2);
        var encode1 = InternalPacks.packIntValues(driverContext, block1);
        var encode2 = InternalPacks.packIntValues(driverContext, block2);
        var decoded1 = InternalPacks.unpackIntValues(driverContext, encode1);
        var decoded2 = InternalPacks.unpackIntValues(driverContext, encode2);
        try {
            assertThat(decoded1, equalTo(block1));
            assertThat(decoded2, equalTo(block2));
            for (int p = 0; p < positionCount; p++) {
                List<?> v1 = values1.get(p);
                List<?> v2 = values2.get(p);
                if (v1.equals(v2)) {
                    assertTrue(encodedRowEquals(encode1, encode2, p));
                } else {
                    assertFalse(encodedRowEquals(encode1, encode2, p));
                }
            }
        } finally {
            Releasables.close(block1, encode1, decoded1, block2, encode2, decoded2);
        }
    }

    public void testBoolean() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
        int positionCount = between(1, 100);
        List<List<?>> values1 = new ArrayList<>();
        List<List<?>> values2 = new ArrayList<>();
        for (int i = 0; i < positionCount; i++) {
            List<Boolean> v = IntStream.of(randomIntBetween(0, 5)).mapToObj(n -> randomBoolean()).toList();
            values1.add(v);
            if (randomBoolean()) {
                values2.add(v);
            } else {
                values2.add(randomValueOtherThan(v, () -> IntStream.of(randomIntBetween(0, 5)).mapToObj(n -> randomBoolean()).toList()));
            }
        }
        var block1 = (BooleanBlock) buildBlock(blockFactory, ElementType.BOOLEAN, values1);
        var block2 = (BooleanBlock) buildBlock(blockFactory, ElementType.BOOLEAN, values2);
        var encode1 = InternalPacks.packBooleanValues(driverContext, block1);
        var encode2 = InternalPacks.packBooleanValues(driverContext, block2);
        var decoded1 = InternalPacks.unpackBooleanValues(driverContext, encode1);
        var decoded2 = InternalPacks.unpackBooleanValues(driverContext, encode2);
        try {
            assertThat(decoded1, equalTo(block1));
            assertThat(decoded2, equalTo(block2));
            for (int p = 0; p < positionCount; p++) {
                List<?> v1 = values1.get(p);
                List<?> v2 = values2.get(p);
                if (v1.equals(v2)) {
                    assertTrue(encodedRowEquals(encode1, encode2, p));
                } else {
                    assertFalse(encodedRowEquals(encode1, encode2, p));
                }
            }
        } finally {
            Releasables.close(block1, encode1, decoded1, block2, encode2, decoded2);
        }
    }

    public void testOrdinal() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
        int dictPosition = between(1, 100);
        final BytesRefVector dict;
        try (BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(dictPosition)) {
            for (int i = 0; i < dictPosition; i++) {
                builder.appendBytesRef(new BytesRef(randomAlphaOfLengthBetween(1, 10)));
            }
            dict = builder.build();
        }
        int positionCount = between(1, 100);
        try (
            var builder = blockFactory.newBytesRefBlockBuilder(positionCount);
            var ordinals = blockFactory.newIntBlockBuilder(positionCount);
        ) {
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < positionCount; i++) {
                int ordinal = randomIntBetween(0, dictPosition - 1);
                builder.appendBytesRef(dict.getBytesRef(ordinal, scratch));
                ordinals.appendInt(ordinal);
            }
            var block1 = builder.build();
            var block2 = new OrdinalBytesRefBlock(ordinals.build(), dict);
            var encoded1 = InternalPacks.packBytesValues(driverContext, block1);
            var encoded2 = InternalPacks.packBytesValues(driverContext, block2);
            var decoded1 = InternalPacks.unpackBytesValues(driverContext, encoded1);
            var decoded2 = InternalPacks.unpackBytesValues(driverContext, encoded2);
            try {
                assertTrue(BytesRefBlock.equals(block1, block2));
                assertTrue(BytesRefBlock.equals(encoded1, encoded2));
                assertTrue(BytesRefBlock.equals(decoded1, decoded2));
            } finally {
                Releasables.close(block1, encoded1, decoded1, block2, encoded2, decoded2);
            }
        }

    }
}
