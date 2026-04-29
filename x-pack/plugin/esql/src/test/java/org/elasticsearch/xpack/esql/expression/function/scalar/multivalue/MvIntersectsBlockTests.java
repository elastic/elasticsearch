/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * As certain paths rely on blocks being constructed with specific properties (mvOrdering) we added a
 * separate test class
 */
public class MvIntersectsBlockTests extends ESTestCase {

    public void testWithOrderedIntMultivalueBlocks() {
        var values = randomListSortedList(ESTestCase::randomInt);

        IntBlock left;
        try (var builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(values.size())) {
            for (var multiValues : values) {
                builder.beginPositionEntry();
                multiValues.forEach(builder::appendInt);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            left = builder.build();
        }

        IntBlock right;
        var size = randomIntBetween(4, 12);
        try (var builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(values.size() * size)) {
            for (var multiValues : values) {
                builder.beginPositionEntry();

                List<Integer> rightValues = new ArrayList<>();
                randomInts().filter(o -> false == multiValues.contains(o)).limit(size).forEach(rightValues::add);
                rightValues.set(randomInt(size - 1), multiValues.get(randomInt(multiValues.size() - 1)));
                Collections.sort(rightValues);

                rightValues.forEach(builder::appendInt);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            right = builder.build();
        }

        for (var i = 0; i < values.size(); i++) {
            assertTrue(MvIntersects.process(i, left, right));
        }
    }

    public void testWithOrderedLongMultivalueBlocks() {
        var values = randomListSortedList(ESTestCase::randomLong);

        LongBlock left;
        try (var builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(values.size())) {
            for (var multiValues : values) {
                builder.beginPositionEntry();
                multiValues.forEach(builder::appendLong);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            left = builder.build();
        }

        LongBlock right;
        var size = randomIntBetween(4, 12);
        try (var builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(values.size() * size)) {
            for (var multiValues : values) {
                builder.beginPositionEntry();

                List<Long> rightValues = new ArrayList<>();
                randomLongs().filter(o -> false == multiValues.contains(o)).limit(size).forEach(rightValues::add);
                rightValues.set(randomInt(size - 1), multiValues.get(randomInt(multiValues.size() - 1)));
                Collections.sort(rightValues);

                rightValues.forEach(builder::appendLong);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            right = builder.build();
        }

        for (var i = 0; i < values.size(); i++) {
            assertTrue(MvIntersects.process(i, left, right));
        }
    }

    public void testWithOrderedDoubleMultivalueBlocks() {
        var values = randomListSortedList(ESTestCase::randomDouble);

        DoubleBlock left;
        try (var builder = TestBlockFactory.getNonBreakingInstance().newDoubleBlockBuilder(values.size())) {
            for (var multiValues : values) {
                builder.beginPositionEntry();
                multiValues.forEach(builder::appendDouble);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            left = builder.build();
        }

        DoubleBlock right;
        var size = randomIntBetween(4, 12);
        try (var builder = TestBlockFactory.getNonBreakingInstance().newDoubleBlockBuilder(values.size() * size)) {
            for (var multiValues : values) {
                builder.beginPositionEntry();

                List<Double> rightValues = new ArrayList<>();
                randomDoubles().filter(o -> false == multiValues.contains(o)).limit(size).forEach(rightValues::add);
                rightValues.set(randomInt(size - 1), multiValues.get(randomInt(multiValues.size() - 1)));
                Collections.sort(rightValues);

                rightValues.forEach(builder::appendDouble);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            right = builder.build();
        }

        for (var i = 0; i < values.size(); i++) {
            assertTrue(MvIntersects.process(i, left, right));
        }
    }

    public void testWithOrderedPointMultivalueBlocks() {
        var values = randomListSortedList(() -> new BytesRef(randomAlphaOfLengthBetween(4, 8)));

        BytesRefBlock left;
        try (var builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(values.size())) {
            for (var multiValues : values) {
                builder.beginPositionEntry();
                multiValues.forEach(builder::appendBytesRef);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            left = builder.build();
        }

        BytesRefBlock right;
        var size = randomIntBetween(4, 12);
        try (var builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(values.size() * size)) {
            for (var multiValues : values) {
                builder.beginPositionEntry();

                List<BytesRef> rightValues = new ArrayList<>();
                randomStringBasedByteRefs().filter(o -> false == multiValues.contains(o)).limit(size).forEach(rightValues::add);
                rightValues.set(randomInt(size - 1), multiValues.get(randomInt(multiValues.size() - 1)));
                Collections.sort(rightValues);

                rightValues.forEach(builder::appendBytesRef);
                builder.endPositionEntry();
            }
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            right = builder.build();
        }

        for (var position = 0; position < values.size(); position++) {
            if (MvIntersects.process(position, left, right)) {
                continue;
            }
            fail(
                "At position '"
                    + position
                    + "':\n"
                    + toString(left, position)
                    + "\n"
                    + "was expected to overlap / intersect with:\n"
                    + toString(right, position)
            );
        }
    }

    private String toString(BytesRefBlock block, int position) {
        var strings = new ArrayList<>();
        var start = block.getFirstValueIndex(position);
        var end = block.getValueCount(position);
        var scratch = new BytesRef();
        for (var index = start; index < start + end; index++) {
            scratch = block.getBytesRef(index, scratch);
            strings.add(scratch.utf8ToString());
        }
        return Objects.toString(strings);
    }

    private Stream<BytesRef> randomStringBasedByteRefs() {
        return Stream.generate(() -> new BytesRef(randomAlphaOfLengthBetween(4, 8)));
    }

    private static <Type extends Comparable<? super Type>> List<List<Type>> randomListSortedList(Supplier<Type> supplier) {
        return randomList(2, 10, () -> {
            List<Type> list = randomList(32, 64, supplier);
            Collections.sort(list);
            return list;
        });
    }

}
