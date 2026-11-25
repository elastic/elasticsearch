/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.RowKeyHash;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder.DoubleRange;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.After;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Compares runtime {@code ==} with grouping, following the contract on {@link DataType#isGroupable()}.
 * A disagreement is a known exception, pinned here, and is not a change to the hash. Types with no
 * {@code ==} evaluator are covered for byte identity by the lookup round trip, and for eligibility by
 * {@link GroupableTypesTests}.
 */
public class GroupingEqualityTests extends ESTestCase {

    private final CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
    private final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
    private final MockBlockFactory blockFactory = new MockBlockFactory(
        BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService))
    );

    @After
    public void checkBreaker() {
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breaker.getUsed(), is(0L));
    }

    /**
     * Groupable types whose {@code ==} result can be the oracle. {@code null}, {@code _source} and {@code _tsid}
     * have no evaluator. {@code dense_vector} compares element by element, in order, and only the row-key hash
     * can group it. Its {@code NaN} and signed-zero exceptions are pinned with the {@code double} ones.
     */
    private static List<DataType> equalityTypes(BlockHashKind kind) {
        return Arrays.stream(DataType.values())
            .filter(DataType::isGroupable)
            .filter(type -> type != DataType.NULL && type != DataType.SOURCE && type != DataType.TSID_DATA_TYPE)
            .filter(type -> type != DataType.DENSE_VECTOR || kind == BlockHashKind.ROW)
            .toList();
    }

    public void testSingleKeyBlockHashAgreesWithEquals() {
        agrees(BlockHashKind.SINGLE);
    }

    public void testRowKeyHashAgreesWithEquals() {
        agrees(BlockHashKind.ROW);
    }

    public void testPackedBlockHashAgreesWithEquals() {
        agrees(BlockHashKind.PACKED);
    }

    /** Infinities are ordinary values: {@code +Infinity == +Infinity}, and it is not {@code -Infinity}. */
    public void testInfinitiesAgreeWithEquals() {
        for (BlockHashKind kind : BlockHashKind.values()) {
            assertAgrees(DataType.DOUBLE, kind, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
            assertAgrees(DataType.DOUBLE, kind, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        }
    }

    /**
     * {@code version}, spatial types and {@code histogram} compare encoded bytes. A copy of those bytes is the
     * same key. A second value this type can produce is a different key.
     */
    public void testEncodedBytesStayDistinct() {
        assertEncodedBytesStayDistinct(DataType.VERSION, new Version("1.2.3").toBytesRef(), new Version("1.2.4").toBytesRef());
        assertEncodedBytesStayDistinct(DataType.GEO_POINT, GEO.asWkb(new Point(0, 0)), GEO.asWkb(new Point(1, 1)));
        assertEncodedBytesStayDistinct(DataType.GEO_SHAPE, GEO.asWkb(new Rectangle(0, 1, 1, 0)), GEO.asWkb(new Rectangle(2, 3, 1, 0)));
        assertEncodedBytesStayDistinct(DataType.CARTESIAN_POINT, CARTESIAN.asWkb(new Point(0, 0)), CARTESIAN.asWkb(new Point(1, 1)));
        assertEncodedBytesStayDistinct(
            DataType.CARTESIAN_SHAPE,
            CARTESIAN.asWkb(new Rectangle(0, 1, 1, 0)),
            CARTESIAN.asWkb(new Rectangle(2, 3, 1, 0))
        );
        assertEncodedBytesStayDistinct(DataType.HISTOGRAM, histogram(1.0d), histogram(2.0d));
    }

    private void assertEncodedBytesStayDistinct(DataType type, BytesRef value, BytesRef other) {
        assertFalse(type.typeName(), value.bytesEquals(other));
        BytesRef copy = BytesRef.deepCopyOf(value);
        assertTrue(type.typeName(), value.bytesEquals(copy));
        for (BlockHashKind kind : BlockHashKind.values()) {
            assertAgrees(type, kind, value, copy);
            assertAgrees(type, kind, value, other);
        }
    }

    /** One bucket, in the layout {@link EsqlTestUtils#randomHistogram()} writes. */
    private static BytesRef histogram(double value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVLong(1);
            out.writeLong(Double.doubleToRawLongBits(value));
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The same bytes in a larger buffer are the same key. A geometry written in the other WKB byte order is a
     * valid encoding of the same shape, and {@code ==} compares those bytes, so it is a different key.
     */
    public void testEquivalentRepresentationsAgreeWithEquals() {
        for (DataType type : List.of(DataType.KEYWORD, DataType.VERSION, DataType.GEO_POINT, DataType.HISTOGRAM)) {
            BytesRef value = encoded(type);
            BytesRef slice = slice(value);
            assertTrue(type.typeName(), value.bytesEquals(slice));
            for (BlockHashKind kind : BlockHashKind.values()) {
                assertAgrees(type, kind, value, slice);
            }
        }
        for (DataType type : List.of(DataType.GEO_POINT, DataType.GEO_SHAPE, DataType.CARTESIAN_POINT, DataType.CARTESIAN_SHAPE)) {
            BytesRef little = wkb(type, ByteOrder.LITTLE_ENDIAN);
            BytesRef big = wkb(type, ByteOrder.BIG_ENDIAN);
            assertFalse(type.typeName(), little.bytesEquals(big));
            assertThat(type.typeName(), wkt(big), equalTo(wkt(little)));
            for (BlockHashKind kind : BlockHashKind.values()) {
                assertAgrees(type, kind, little, big);
            }
        }
    }

    /**
     * The same key is one group whichever page it arrives on, and whichever row it occupies on that page.
     * A multivalue list keeps its own order: {@code [1, 2]} on a later page matches {@code [1, 2]}, not {@code [2, 1]}.
     */
    public void testGroupingIgnoresPageAndOrder() {
        for (BlockHashKind kind : BlockHashKind.values()) {
            try (Block earlier = ints(2, 1); Block later = ints(1, 3, 2)) {
                assertSameKeyAcrossPages(kind, ElementType.INT, earlier, later, 1, 0, 1);
            }
            try (Block earlier = keywords("b", "a"); Block later = keywords("a", "c", "b")) {
                assertSameKeyAcrossPages(kind, ElementType.BYTES_REF, earlier, later, 1, 0, 1);
            }
            try (
                Block earlier = blockFactory.newIntBlockBuilder(2).appendInt(1).appendNull().build();
                Block later = blockFactory.newIntBlockBuilder(2).appendNull().appendInt(1).build()
            ) {
                assertSameKeyAcrossPages(kind, ElementType.INT, earlier, later, 1, 0, -1);
            }
        }
        try (
            Block earlier = blockFactory.newIntBlockBuilder(4)
                .beginPositionEntry()
                .appendInt(1)
                .appendInt(2)
                .endPositionEntry()
                .beginPositionEntry()
                .appendInt(2)
                .appendInt(1)
                .endPositionEntry()
                .build();
            Block later = blockFactory.newIntBlockBuilder(4)
                .beginPositionEntry()
                .appendInt(2)
                .appendInt(1)
                .endPositionEntry()
                .beginPositionEntry()
                .appendInt(1)
                .appendInt(2)
                .endPositionEntry()
                .build()
        ) {
            int[] first;
            int[] second;
            try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(ElementType.INT), blockFactory)) {
                first = hash.add(new Page(earlier));
                second = hash.add(new Page(later));
            }
            assertThat(second[0], equalTo(first[1]));
            assertThat(second[1], equalTo(first[0]));
            assertThat(first[0], not(equalTo(first[1])));
        }
        try (
            Block numbers = ints(1, 2);
            Block names = keywords("a", "b");
            Block laterNumbers = ints(2, 1);
            Block laterNames = keywords("b", "a")
        ) {
            try (RowKeyHash hash = new RowKeyHash(new int[] { 0, 1 }, List.of(ElementType.INT, ElementType.BYTES_REF), blockFactory)) {
                int[] first = hash.add(new Page(numbers, names));
                int[] second = hash.add(new Page(laterNumbers, laterNames));
                assertThat(second[0], equalTo(first[1]));
                assertThat(second[1], equalTo(first[0]));
                assertThat(first[0], not(equalTo(first[1])));
            }
            try (
                BlockHash hash = BlockHash.buildPackedValuesBlockHash(
                    List.of(new BlockHash.GroupSpec(0, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                    blockFactory,
                    1024
                )
            ) {
                int[][] first = collect(hash, new Page(numbers, names), false);
                int[][] second = collect(hash, new Page(laterNumbers, laterNames), false);
                assertThat(second[0][0], equalTo(first[1][0]));
                assertThat(second[1][0], equalTo(first[0][0]));
                assertThat(first[0][0], not(equalTo(first[1][0])));
            }
        }
    }

    /**
     * A block read back from the wire is the same key as the block that was written. This includes a multivalue list
     * and the raw bits of a NaN or a signed zero.
     */
    public void testSerializedBlockGroupsWithTheOriginal() {
        for (BlockHashKind kind : BlockHashKind.values()) {
            for (DataType type : equalityTypes(kind)) {
                try (Block original = valueBlock(blockFactory, ordinary(type)); Block restored = roundTrip(original)) {
                    assertBlocksShareGroups(kind, PlannerUtils.toElementType(type), original, restored);
                }
            }
        }
        try (Block original = pairBlock(blockFactory, 1, null); Block restored = roundTrip(original)) {
            for (BlockHashKind kind : BlockHashKind.values()) {
                assertBlocksShareGroups(kind, ElementType.INT, original, restored);
            }
        }
        try (
            Block original = blockFactory.newIntBlockBuilder(2)
                .beginPositionEntry()
                .appendInt(1)
                .appendInt(2)
                .endPositionEntry()
                .beginPositionEntry()
                .appendInt(2)
                .appendInt(1)
                .endPositionEntry()
                .build();
            Block restored = roundTrip(original)
        ) {
            for (BlockHashKind kind : BlockHashKind.values()) {
                assertBlocksShareGroups(kind, ElementType.INT, original, restored);
            }
        }
        // Block serialization writes a double with StreamOutput#writeDouble, which uses doubleToLongBits.
        // That collapses every NaN payload into one, and collapses a signed zero exactly when doubleToLongBits does.
        // https://github.com/elastic/elasticsearch/issues/167086
        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        try (Block original = pairBlock(blockFactory, Double.NaN, otherNan); Block restored = roundTrip(original)) {
            assertDoubleRoundTrip(original, restored, true);
        }
        boolean signedZeroCollapses = Double.doubleToLongBits(0.0d) == Double.doubleToLongBits(-0.0d);
        try (Block original = pairBlock(blockFactory, 0.0d, -0.0d); Block restored = roundTrip(original)) {
            assertDoubleRoundTrip(original, restored, signedZeroCollapses);
        }
    }

    /**
     * A list, a block builder, a constant vector, and a copy of the block are the same key. A null position built
     * twice, and a copy of it, are the same key too.
     */
    public void testProducersAgreeOnTheGroup() {
        try (
            Block listed = valueBlock(blockFactory, 7);
            Block built = blockFactory.newIntBlockBuilder(1).appendInt(7).build();
            Block constant = blockFactory.newConstantIntBlockWith(7, 1);
            Block copied = BlockUtils.deepCopyOf(listed, blockFactory)
        ) {
            assertProducersShareGroup(ElementType.INT, listed, built, constant, copied);
        }
        try (
            Block listed = valueBlock(blockFactory, 7L);
            Block built = blockFactory.newLongBlockBuilder(1).appendLong(7L).build();
            Block constant = blockFactory.newConstantLongBlockWith(7L, 1);
            Block copied = BlockUtils.deepCopyOf(listed, blockFactory)
        ) {
            assertProducersShareGroup(ElementType.LONG, listed, built, constant, copied);
        }
        try (
            Block listed = valueBlock(blockFactory, 1.5d);
            Block built = blockFactory.newDoubleBlockBuilder(1).appendDouble(1.5d).build();
            Block constant = blockFactory.newConstantDoubleBlockWith(1.5d, 1);
            Block copied = BlockUtils.deepCopyOf(listed, blockFactory)
        ) {
            assertProducersShareGroup(ElementType.DOUBLE, listed, built, constant, copied);
        }
        try (
            Block listed = valueBlock(blockFactory, true);
            Block built = blockFactory.newBooleanBlockBuilder(1).appendBoolean(true).build();
            Block constant = blockFactory.newConstantBooleanBlockWith(true, 1);
            Block copied = BlockUtils.deepCopyOf(listed, blockFactory)
        ) {
            assertProducersShareGroup(ElementType.BOOLEAN, listed, built, constant, copied);
        }
        BytesRef text = new BytesRef("keyword");
        try (
            Block listed = valueBlock(blockFactory, text);
            Block built = blockFactory.newBytesRefBlockBuilder(1).appendBytesRef(text).build();
            Block constant = blockFactory.newConstantBytesRefBlockWith(text, 1);
            Block copied = BlockUtils.deepCopyOf(listed, blockFactory)
        ) {
            assertProducersShareGroup(ElementType.BYTES_REF, listed, built, constant, copied);
        }
        DoubleRange range = new DoubleRange(1d, 3d);
        try (
            Block listed = valueBlock(blockFactory, range);
            Block built = blockFactory.newDoubleRangeBlockBuilder(1).appendDoubleRange(range).build();
            Block constant = blockFactory.newConstantDoubleRangeBlockWith(range, 1);
            Block copied = BlockUtils.deepCopyOf(listed, blockFactory)
        ) {
            assertProducersShareGroup(ElementType.DOUBLE_RANGE, listed, built, constant, copied);
        }
        try (
            Block built = blockFactory.newIntBlockBuilder(1).appendNull().build();
            Block again = blockFactory.newIntBlockBuilder(1).appendNull().build();
            Block copied = BlockUtils.deepCopyOf(built, blockFactory)
        ) {
            assertProducersShareGroup(ElementType.INT, built, again, copied);
        }
    }

    private void assertProducersShareGroup(ElementType type, Block... blocks) {
        for (BlockHashKind kind : BlockHashKind.values()) {
            if (kind == BlockHashKind.ROW) {
                try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(type), blockFactory)) {
                    int first = hash.add(new Page(blocks[0]))[0];
                    for (int i = 1; i < blocks.length; i++) {
                        assertThat(hash.add(new Page(blocks[i]))[0], equalTo(first));
                    }
                }
            } else {
                BlockHash hash = kind == BlockHashKind.SINGLE
                    ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024, false)
                    : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024);
                try {
                    Set<Integer> first = set(collect(hash, new Page(blocks[0]), false)[0]);
                    for (int i = 1; i < blocks.length; i++) {
                        assertThat(set(collect(hash, new Page(blocks[i]), false)[0]), equalTo(first));
                    }
                } finally {
                    hash.close();
                }
            }
        }
    }

    /**
     * Hash the original page and then the restored page. When {@code collapseDistinctBits} is true, a non-single
     * hash kept the two positions apart and the restored page maps both of them onto the first position's group.
     */
    private void assertDoubleRoundTrip(Block original, Block restored, boolean collapseDistinctBits) {
        for (BlockHashKind kind : BlockHashKind.values()) {
            if (collapseDistinctBits && kind != BlockHashKind.SINGLE) {
                int[][] groups = groupedPositions(kind, ElementType.DOUBLE, original, restored);
                assertThat(set(groups[0]), not(equalTo(set(groups[1]))));
                assertThat(set(groups[2]), equalTo(set(groups[0])));
                assertThat(set(groups[3]), equalTo(set(groups[0])));
            } else {
                assertBlocksShareGroups(kind, ElementType.DOUBLE, original, restored);
            }
        }
    }

    /** The restored block was written from {@code original}, so each position keeps that position's groups. */
    private void assertBlocksShareGroups(BlockHashKind kind, ElementType type, Block original, Block restored) {
        if (kind == BlockHashKind.ROW) {
            try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(type), blockFactory)) {
                int[] first = hash.add(new Page(original));
                int[] second = hash.add(new Page(restored));
                assertThat(Arrays.toString(second), equalTo(Arrays.toString(first)));
            }
            return;
        }
        BlockHash hash = kind == BlockHashKind.SINGLE
            ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024, false)
            : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024);
        try {
            int[][] first = collect(hash, new Page(original), false);
            int[][] second = collect(hash, new Page(restored), false);
            for (int position = 0; position < first.length; position++) {
                assertThat(set(second[position]), equalTo(set(first[position])));
            }
        } finally {
            hash.close();
        }
    }

    /** Groups of the original page's positions, then the restored page's, from one hash. */
    private int[][] groupedPositions(BlockHashKind kind, ElementType type, Block original, Block restored) {
        if (kind == BlockHashKind.ROW) {
            try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(type), blockFactory)) {
                int[] first = hash.add(new Page(original));
                int[] second = hash.add(new Page(restored));
                return new int[][] { { first[0] }, { first[1] }, { second[0] }, { second[1] } };
            }
        }
        BlockHash hash = kind == BlockHashKind.SINGLE
            ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024, false)
            : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024);
        try {
            int[][] first = collect(hash, new Page(original), false);
            int[][] second = collect(hash, new Page(restored), false);
            return new int[][] { first[0], first[1], second[0], second[1] };
        } finally {
            hash.close();
        }
    }

    private Block roundTrip(Block block) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Block.writeTypedBlock(block, out);
            try (
                BlockStreamInput in = new BlockStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), blockFactory)
            ) {
                return Block.readTypedBlock(in);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * {@code earlierMatch} on the first page is the same key as position 0 of the second page, and
     * {@code laterMatch} is the same key as its last position. {@code fresh} is a new key, or {@code -1} when the
     * second page only repeats keys.
     */
    private void assertSameKeyAcrossPages(
        BlockHashKind kind,
        ElementType type,
        Block earlier,
        Block later,
        int earlierMatch,
        int laterMatch,
        int fresh
    ) {
        if (kind == BlockHashKind.ROW) {
            try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(type), blockFactory)) {
                int[] first = hash.add(new Page(earlier));
                int[] second = hash.add(new Page(later));
                assertThat(kind + " " + type, first[0], not(equalTo(first[1])));
                assertThat(kind + " " + type, second[0], equalTo(first[earlierMatch]));
                assertThat(kind + " " + type, second[second.length - 1], equalTo(first[laterMatch]));
                if (fresh >= 0) {
                    assertThat(second[fresh], not(equalTo(first[0])));
                    assertThat(second[fresh], not(equalTo(first[1])));
                }
            }
            return;
        }
        try (
            BlockHash hash = kind == BlockHashKind.SINGLE
                ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024, false)
                : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, type)), blockFactory, 1024)
        ) {
            int[][] first = collect(hash, new Page(earlier), false);
            int[][] second = collect(hash, new Page(later), false);
            assertOneIdEach(first);
            assertOneIdEach(second);
            assertThat(kind + " " + type, first[0][0], not(equalTo(first[1][0])));
            assertThat(kind + " " + type, second[0][0], equalTo(first[earlierMatch][0]));
            assertThat(kind + " " + type, second[second.length - 1][0], equalTo(first[laterMatch][0]));
            if (fresh >= 0) {
                assertThat(second[fresh][0], not(equalTo(first[0][0])));
                assertThat(second[fresh][0], not(equalTo(first[1][0])));
            }
        }
    }

    private Block ints(int... values) {
        var builder = blockFactory.newIntBlockBuilder(values.length);
        for (int value : values) {
            builder.appendInt(value);
        }
        return builder.build();
    }

    private Block keywords(String... values) {
        var builder = blockFactory.newBytesRefBlockBuilder(values.length);
        for (String value : values) {
            builder.appendBytesRef(new BytesRef(value));
        }
        return builder.build();
    }

    private static BytesRef encoded(DataType type) {
        return switch (type) {
            case KEYWORD -> new BytesRef("keyword");
            case VERSION -> new Version("1.2.3").toBytesRef();
            case GEO_POINT -> GEO.asWkb(new Point(1, 2));
            case HISTOGRAM -> histogram(1.0d);
            default -> throw new AssertionError(type);
        };
    }

    /** The same bytes, starting at a non-zero offset of a larger array. */
    private static BytesRef slice(BytesRef value) {
        byte[] padded = new byte[value.length + 4];
        System.arraycopy(value.bytes, value.offset, padded, 2, value.length);
        return new BytesRef(padded, 2, value.length);
    }

    private static BytesRef wkb(DataType type, ByteOrder order) {
        var geometry = switch (type) {
            case GEO_POINT, CARTESIAN_POINT -> new Point(1, 2);
            case GEO_SHAPE, CARTESIAN_SHAPE -> new Rectangle(0, 1, 1, 0);
            default -> throw new AssertionError(type);
        };
        return new BytesRef(WellKnownBinary.toWKB(geometry, order));
    }

    private static String wkt(BytesRef wkb) {
        return WellKnownText.fromWKB(wkb.bytes, wkb.offset, wkb.length);
    }

    /**
     * A {@code double_range} bound uses {@link Double#compare(double, double)}, so {@code ==} matches single-key
     * {@code STATS}. The other hashes keep the raw bits and can split a bound those two treat as one value.
     * Tracked by https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testDoubleRangeBoundsStayAsTheyAre() {
        DoubleRange positive = new DoubleRange(0.0d, 1.0d);
        DoubleRange negative = new DoubleRange(-0.0d, 1.0d);
        boolean sameBits = Double.doubleToLongBits(-0.0d) == Double.doubleToLongBits(0.0d);
        assertThat(valuesEqual(DataType.DOUBLE_RANGE, positive, negative), equalTo(Double.compare(0.0d, -0.0d) == 0));
        assertThat(sameGroup(DataType.DOUBLE_RANGE, positive, negative, BlockHashKind.SINGLE), equalTo(sameBits));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, positive, negative, BlockHashKind.ROW));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, positive, negative, BlockHashKind.PACKED));

        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        DoubleRange nan = new DoubleRange(Double.NaN, 1.0d);
        DoubleRange other = new DoubleRange(otherNan, 1.0d);
        assertTrue(valuesEqual(DataType.DOUBLE_RANGE, nan, other));
        assertTrue(sameGroup(DataType.DOUBLE_RANGE, nan, other, BlockHashKind.SINGLE));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, nan, other, BlockHashKind.ROW));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, nan, other, BlockHashKind.PACKED));
    }

    /**
     * Composite keys group when every column matches. {@code (1, a)} matches itself and not {@code (1, b)} or
     * {@code (2, a)}.
     */
    public void testCompositeKeyGroupsByEveryColumn() {
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        try (
            Block numbers = blockFactory.newIntBlockBuilder(4).appendInt(1).appendInt(1).appendInt(1).appendInt(2).build();
            Block names = blockFactory.newBytesRefBlockBuilder(4)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(b)
                .appendBytesRef(a)
                .build();
            BlockHash hash = BlockHash.buildPackedValuesBlockHash(
                List.of(new BlockHash.GroupSpec(0, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                blockFactory,
                1024
            )
        ) {
            // (1, a), (1, a), (1, b), (2, a)
            int[][] ids = collect(hash, new Page(numbers, names), false);
            for (int[] id : ids) {
                assertThat(id.length, equalTo(1));
            }
            assertThat(ids[1][0], equalTo(ids[0][0]));
            assertThat(ids[2][0], not(equalTo(ids[0][0])));
            assertThat(ids[3][0], not(equalTo(ids[0][0])));
            assertThat(ids[2][0], not(equalTo(ids[3][0])));
        }
    }

    /**
     * Multi-key {@code STATS} keeps {@code NaN} payloads and the sign of zero apart. {@code NaN == NaN} is false, so
     * the payloads agree with {@code ==}. {@code 0.0 == -0.0} is true, so the sign does not. Tracked by
     * https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testCompositeKeyFloatBitsStayRaw() {
        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        try (
            Block ones = blockFactory.newIntBlockBuilder(4).appendInt(1).appendInt(1).appendInt(1).appendInt(2).build();
            Block nans = blockFactory.newDoubleBlockBuilder(4)
                .appendDouble(Double.NaN)
                .appendDouble(Double.NaN)
                .appendDouble(otherNan)
                .appendDouble(Double.NaN)
                .build();
            BlockHash nanHash = BlockHash.buildPackedValuesBlockHash(
                List.of(new BlockHash.GroupSpec(0, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.DOUBLE)),
                blockFactory,
                1024
            )
        ) {
            // (1, NaN), (1, NaN), (1, other NaN), (2, NaN)
            int[][] ids = collect(nanHash, new Page(ones, nans), false);
            assertOneIdEach(ids);
            assertThat(ids[1][0], equalTo(ids[0][0]));
            assertThat(ids[2][0], not(equalTo(ids[0][0])));
            assertThat(ids[3][0], not(equalTo(ids[0][0])));
            assertThat(ids[3][0], not(equalTo(ids[2][0])));
        }
        assertFalse(valuesEqual(DataType.DOUBLE, Double.NaN, otherNan));
        try (
            Block ones = blockFactory.newIntBlockBuilder(4).appendInt(1).appendInt(1).appendInt(1).appendInt(2).build();
            Block zeros = blockFactory.newDoubleBlockBuilder(4)
                .appendDouble(0.0d)
                .appendDouble(0.0d)
                .appendDouble(-0.0d)
                .appendDouble(0.0d)
                .build();
            BlockHash zeroHash = BlockHash.buildPackedValuesBlockHash(
                List.of(new BlockHash.GroupSpec(0, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.DOUBLE)),
                blockFactory,
                1024
            )
        ) {
            // (1, 0.0), (1, 0.0), (1, -0.0), (2, 0.0)
            int[][] ids = collect(zeroHash, new Page(ones, zeros), false);
            assertOneIdEach(ids);
            assertThat(ids[1][0], equalTo(ids[0][0]));
            assertThat(ids[2][0], not(equalTo(ids[0][0])));
            assertThat(ids[3][0], not(equalTo(ids[0][0])));
            assertThat(ids[3][0], not(equalTo(ids[2][0])));
        }
        assertTrue(valuesEqual(DataType.DOUBLE, 0.0d, -0.0d));
    }

    /**
     * {@code LIMIT BY} and {@code DEDUP} group a composite key when every column matches. A multivalue stays
     * whole and in order, so {@code ([1, 2], a)} matches itself and not {@code ([2, 1], a)} or {@code (1, a)}.
     * A null matches only when every column does.
     */
    public void testCompositeRowKeyGroupsByEveryColumn() {
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        try (
            Block numbers = blockFactory.newIntBlockBuilder(12)
                .appendInt(1)
                .appendInt(1)
                .appendInt(1)
                .appendInt(2)
                .beginPositionEntry()
                .appendInt(1)
                .appendInt(2)
                .endPositionEntry()
                .beginPositionEntry()
                .appendInt(2)
                .appendInt(1)
                .endPositionEntry()
                .beginPositionEntry()
                .appendInt(1)
                .appendInt(2)
                .endPositionEntry()
                .beginPositionEntry()
                .appendInt(1)
                .appendInt(1)
                .endPositionEntry()
                .appendNull()
                .appendNull()
                .appendNull()
                .appendInt(1)
                .build();
            Block names = blockFactory.newBytesRefBlockBuilder(12)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(b)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(b)
                .appendNull()
                .build()
        ) {
            // (1, a), (1, a), (1, b), (2, a), ([1, 2], a), ([2, 1], a), ([1, 2], a), ([1, 1], a),
            // (null, a), (null, a), (null, b), (1, null)
            int[] ids = rowKeyGroups(List.of(ElementType.INT, ElementType.BYTES_REF), numbers, names);
            assertThat(ids[1], equalTo(ids[0]));
            assertThat(ids[2], not(equalTo(ids[0])));
            assertThat(ids[3], not(equalTo(ids[0])));
            assertThat(ids[2], not(equalTo(ids[3])));
            assertThat(ids[6], equalTo(ids[4]));
            assertThat(ids[5], not(equalTo(ids[4])));
            assertThat(ids[4], not(equalTo(ids[0])));
            assertThat(ids[7], not(equalTo(ids[0])));
            assertThat(ids[9], equalTo(ids[8]));
            assertThat(ids[10], not(equalTo(ids[8])));
            assertThat(ids[11], not(equalTo(ids[0])));
            assertThat(ids[11], not(equalTo(ids[8])));
        }
    }

    /**
     * A composite {@code LIMIT BY} or {@code DEDUP} key keeps {@code NaN} payloads and the sign of zero apart,
     * as the packed hash does. Tracked by https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testCompositeRowKeyFloatBitsStayRaw() {
        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        try (
            Block ones = blockFactory.newIntBlockBuilder(4).appendInt(1).appendInt(1).appendInt(1).appendInt(2).build();
            Block nans = blockFactory.newDoubleBlockBuilder(4)
                .appendDouble(Double.NaN)
                .appendDouble(Double.NaN)
                .appendDouble(otherNan)
                .appendDouble(Double.NaN)
                .build()
        ) {
            // (1, NaN), (1, NaN), (1, other NaN), (2, NaN)
            int[] ids = rowKeyGroups(List.of(ElementType.INT, ElementType.DOUBLE), ones, nans);
            assertThat(ids[1], equalTo(ids[0]));
            assertThat(ids[2], not(equalTo(ids[0])));
            assertThat(ids[3], not(equalTo(ids[0])));
            assertThat(ids[3], not(equalTo(ids[2])));
        }
        assertFalse(valuesEqual(DataType.DOUBLE, Double.NaN, otherNan));
        try (
            Block ones = blockFactory.newIntBlockBuilder(4).appendInt(1).appendInt(1).appendInt(1).appendInt(2).build();
            Block zeros = blockFactory.newDoubleBlockBuilder(4)
                .appendDouble(0.0d)
                .appendDouble(0.0d)
                .appendDouble(-0.0d)
                .appendDouble(0.0d)
                .build()
        ) {
            // (1, 0.0), (1, 0.0), (1, -0.0), (2, 0.0)
            int[] ids = rowKeyGroups(List.of(ElementType.INT, ElementType.DOUBLE), ones, zeros);
            assertThat(ids[1], equalTo(ids[0]));
            assertThat(ids[2], not(equalTo(ids[0])));
            assertThat(ids[3], not(equalTo(ids[0])));
            assertThat(ids[3], not(equalTo(ids[2])));
        }
        assertTrue(valuesEqual(DataType.DOUBLE, 0.0d, -0.0d));
    }

    /** One group id per row from the {@code LIMIT BY} and {@code DEDUP} hash. */
    private int[] rowKeyGroups(List<ElementType> types, Block... blocks) {
        int[] channels = new int[blocks.length];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = i;
        }
        try (RowKeyHash hash = new RowKeyHash(channels, types, blockFactory)) {
            return hash.add(new Page(blocks));
        }
    }

    private static void assertOneIdEach(int[][] ids) {
        for (int[] id : ids) {
            assertThat(id.length, equalTo(1));
        }
    }

    /**
     * {@code null == null} and {@code null == v} fold to null. A multivalue is not a boolean result either:
     * {@code ==} returns null, and the evaluator warns that it saw more than one value.
     */
    public void testNullAndMultivalueEquality() {
        for (DataType type : List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.KEYWORD)) {
            assertNull(type.typeName(), foldEquals(type, null, null));
            assertNull(type.typeName(), foldEquals(type, ordinary(type), null));
        }
        assertNull(foldEquals(DataType.INTEGER, List.of(1, 2), 1));
        assertWarnings(MULTIVALUE_EQUALS_WARNINGS);
        assertNull(foldEquals(DataType.INTEGER, List.of(1, 2), List.of(2, 1)));
        assertWarnings(MULTIVALUE_EQUALS_WARNINGS);
    }

    /**
     * Nulls share a group on every hash. {@code ==} of null is null, so this is not the equality oracle.
     */
    public void testNullsShareAGroup() {
        for (DataType type : List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.KEYWORD)) {
            assertNull(type.typeName(), evaluateEquals(type, null, null));
            Object value = ordinary(type);
            assertNull(type.typeName(), evaluateEquals(type, value, null));
            for (BlockHashKind kind : BlockHashKind.values()) {
                assertTrue(type.typeName() + " " + kind, sameGroup(type, null, null, kind));
                assertFalse(type.typeName() + " " + kind, sameGroup(type, value, null, kind));
            }
        }
    }

    public void testRowKeyGroupsTypesWithoutEquals() {
        assertTrue(sameGroup(DataType.NULL, null, null, BlockHashKind.ROW));
        for (DataType type : List.of(DataType.SOURCE, DataType.TSID_DATA_TYPE)) {
            BytesRef value = new BytesRef("a");
            assertTrue(type.typeName(), sameGroup(type, value, BytesRef.deepCopyOf(value), BlockHashKind.ROW));
            assertFalse(type.typeName(), sameGroup(type, value, new BytesRef("b"), BlockHashKind.ROW));
        }
    }

    /**
     * {@code LIMIT BY} and {@code DEDUP} keep a multivalue whole and in order. {@code [1, 2]} matches itself and
     * not {@code [2, 1]} or {@code 1}.
     */
    public void testRowKeyKeepsMultivalueOrder() {
        try (Block ints = intMultivalue(blockFactory); Block keywords = keywordMultivalue(blockFactory)) {
            assertWhole(ints);
            assertWhole(keywords);
        }
    }

    /**
     * {@code dense_vector} compares element by element, in order, with floating-point {@code ==}. {@link BlockHash}
     * cannot group it, so {@code STATS} and {@code INLINE STATS} reject it. {@code LIMIT BY} and {@code DEDUP}
     * encode that order and keep the raw bits, so a {@code NaN} element groups with the same bits even though
     * {@code NaN == NaN} is false, and {@code 0.0} stays apart from {@code -0.0}. Tracked by
     * https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testDenseVectorRowKeyUsesElementOrder() {
        List<Float> forward = List.of(1f, 2f);
        List<Float> reversed = List.of(2f, 1f);
        assertTrue(valuesEqual(DataType.DENSE_VECTOR, forward, forward));
        assertFalse(valuesEqual(DataType.DENSE_VECTOR, forward, reversed));
        try (
            Block block = floatVectors(blockFactory);
            RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(ElementType.FLOAT), blockFactory)
        ) {
            int[] ids = hash.add(new Page(block));
            assertThat(ids[1], equalTo(ids[0]));
            assertThat(ids[2], not(equalTo(ids[0])));
        }
        List<Float> nan = List.of(Float.NaN);
        float otherNan = Float.intBitsToFloat(Float.floatToRawIntBits(Float.NaN) | 1);
        assertFalse(valuesEqual(DataType.DENSE_VECTOR, nan, nan));
        assertTrue(sameGroup(DataType.DENSE_VECTOR, nan, nan, BlockHashKind.ROW));
        assertFalse(valuesEqual(DataType.DENSE_VECTOR, nan, List.of(otherNan)));
        assertFalse(sameGroup(DataType.DENSE_VECTOR, nan, List.of(otherNan), BlockHashKind.ROW));
        assertTrue(valuesEqual(DataType.DENSE_VECTOR, List.of(0.0f), List.of(-0.0f)));
        assertFalse(sameGroup(DataType.DENSE_VECTOR, List.of(0.0f), List.of(-0.0f), BlockHashKind.ROW));
        expectThrows(
            IllegalArgumentException.class,
            () -> BlockHash.build(List.of(new BlockHash.GroupSpec(0, ElementType.FLOAT)), blockFactory, 16, false).close()
        );
    }

    /**
     * The {@code INLINE STATS} join looks keys up in a {@link RowInTableLookup} built from the grouped output.
     * Probing the original rows, including a repeated key, finds the group that hashing assigned.
     */
    public void testLookupRoundTripFindsOriginalRows() {
        for (DataType type : equalityTypes(BlockHashKind.SINGLE)) {
            ElementType elementType = PlannerUtils.toElementType(type);
            try (Block input = repeatedKeyBlock(elementType, blockFactory)) {
                assertRoundTrip(
                    type.typeName() + " single",
                    input,
                    BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024, false)
                );
                assertRoundTrip(
                    type.typeName() + " packed",
                    input,
                    BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024)
                );
            }
        }
        for (DataType type : List.of(DataType.NULL, DataType.SOURCE, DataType.TSID_DATA_TYPE)) {
            ElementType elementType = PlannerUtils.toElementType(type);
            try (Block input = repeatedKeyBlock(elementType, blockFactory)) {
                assertRoundTrip(
                    type.typeName() + " single",
                    input,
                    BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024, false)
                );
                assertRoundTrip(
                    type.typeName() + " packed",
                    input,
                    BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024)
                );
            }
        }
    }

    /**
     * A multivalue applies to the {@code STATS} hashes. They split it, the grouped keys are single values, and
     * probing the original page finds every group that position was given. The lookup rejects a multivalued
     * build key. {@code LIMIT BY} and {@code DEDUP} keep the list whole, and that hash does not look rows up.
     */
    public void testLookupRoundTripFindsSplitMultivalues() {
        for (BlockHashKind kind : List.of(BlockHashKind.SINGLE, BlockHashKind.PACKED)) {
            for (ElementType elementType : List.of(
                ElementType.BOOLEAN,
                ElementType.INT,
                ElementType.LONG,
                ElementType.DOUBLE,
                ElementType.BYTES_REF,
                ElementType.DOUBLE_RANGE
            )) {
                try (Block block = multivalue(elementType)) {
                    assertSplit(kind, block);
                    BlockHash hash = kind == BlockHashKind.SINGLE
                        ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024, false)
                        : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024);
                    assertRoundTrip(elementType + " " + kind, block, hash);
                }
            }
        }
    }

    /**
     * Records the current {@code NaN} and signed-zero grouping, which disagrees with {@code ==}.
     * Single-key {@code STATS} hashes with {@link Double#doubleToLongBits}, so {@code NaN} payloads share a group.
     * {@code LIMIT BY}, {@code DEDUP} and the packed hash keep the raw bits, so those payloads do not. The sign of
     * zero follows the same split. Tracked by https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testNanAndSignedZeroStayAsTheyAre() {
        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        assertFalse(valuesEqual(DataType.DOUBLE, Double.NaN, otherNan));
        assertTrue(sameGroup(BlockHashKind.SINGLE, Double.NaN, otherNan));
        assertFalse(sameGroup(BlockHashKind.ROW, Double.NaN, otherNan));
        assertFalse(sameGroup(BlockHashKind.PACKED, Double.NaN, otherNan));

        assertTrue(valuesEqual(DataType.DOUBLE, 0.0d, -0.0d));
        assertThat(
            "single-key STATS follows doubleToLongBits",
            sameGroup(BlockHashKind.SINGLE, 0.0d, -0.0d),
            equalTo(Double.doubleToLongBits(0.0d) == Double.doubleToLongBits(-0.0d))
        );
        assertFalse(sameGroup(BlockHashKind.ROW, 0.0d, -0.0d));
        assertFalse(sameGroup(BlockHashKind.PACKED, 0.0d, -0.0d));
    }

    private void agrees(BlockHashKind kind) {
        for (DataType type : equalityTypes(kind)) {
            for (int i = 0; i < 8; i++) {
                Object left = ordinary(type);
                Object right = ordinary(type);
                assertAgrees(type, kind, left, left);
                assertAgrees(type, kind, left, right);
            }
        }
    }

    /** A value {@code ==} can judge. {@code NaN} and {@code -0.0} are pinned separately. */
    private static Object ordinary(DataType type) {
        for (int i = 0; i < 20; i++) {
            Object value = EsqlTestUtils.randomLiteralValue(type);
            if (skipsFloatBits(value)) {
                continue;
            }
            return value;
        }
        throw new AssertionError(type);
    }

    /** {@code NaN} and a negative zero are pinned on their own, including inside a {@code dense_vector}. */
    private static boolean skipsFloatBits(Object value) {
        if (value instanceof Double d) {
            return Double.isFinite(d) == false || Double.doubleToRawLongBits(d) == Double.doubleToRawLongBits(-0.0d);
        }
        if (value instanceof List<?> values) {
            for (Object element : values) {
                if (element instanceof Float f
                    && (Float.isFinite(f) == false || Float.floatToRawIntBits(f) == Float.floatToRawIntBits(-0.0f))) {
                    return true;
                }
            }
        }
        return false;
    }

    private void assertAgrees(DataType type, BlockHashKind kind, Object left, Object right) {
        boolean equal = valuesEqual(type, left, right);
        assertThat(type.typeName() + " " + kind + " [" + left + "] [" + right + "]", sameGroup(type, left, right, kind), equalTo(equal));
    }

    private static final String[] MULTIVALUE_EQUALS_WARNINGS = new String[] {
        "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
        "Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value" };

    private enum BlockHashKind {
        /** Single-key {@code STATS}, {@link BlockHash}. */
        SINGLE,
        /** {@code LIMIT BY} and {@code DEDUP}, {@link RowKeyHash}. */
        ROW,
        /** Multi-key {@code STATS} and the {@code INLINE STATS} join, {@link BlockHash#buildPackedValuesBlockHash}. */
        PACKED
    }

    /** Runtime {@code ==} on one page of blocks. Constant folding must report the same boolean. */
    private boolean valuesEqual(DataType type, Object left, Object right) {
        Boolean runtime = evaluateEquals(type, left, right);
        assertNotNull(type.typeName(), runtime);
        assertThat(type.typeName() + " fold", foldEquals(type, left, right), equalTo(runtime));
        return runtime;
    }

    private Boolean evaluateEquals(DataType type, Object left, Object right) {
        BlockFactory factory = blockFactory;
        DriverContext context = new DriverContext(factory.bigArrays(), factory, null);
        Equals equals = new Equals(Source.EMPTY, new ChannelValue(type, 0), new ChannelValue(type, 1));
        try (
            Block lhs = valueBlock(factory, left);
            Block rhs = valueBlock(factory, right);
            ExpressionEvaluator evaluator = equals.toEvaluator(new EvaluatorMapper.ToEvaluator() {
                @Override
                public ExpressionEvaluator.Factory apply(Expression expression) {
                    return ((EvaluatorMapper) expression).toEvaluator(this);
                }

                @Override
                public FoldContext foldCtx() {
                    return FoldContext.small();
                }
            }).get(context);
            Block result = evaluator.eval(new Page(lhs, rhs))
        ) {
            BooleanBlock booleans = (BooleanBlock) result;
            if (booleans.isNull(0) || booleans.getValueCount(0) != 1) {
                return null;
            }
            return booleans.getBoolean(booleans.getFirstValueIndex(0));
        }
    }

    private static Object foldEquals(DataType type, Object left, Object right) {
        return new Equals(Source.EMPTY, new Literal(Source.EMPTY, left, type), new Literal(Source.EMPTY, right, type)).fold(
            FoldContext.small()
        );
    }

    private boolean sameGroup(BlockHashKind kind, double left, double right) {
        return sameGroup(DataType.DOUBLE, left, right, kind);
    }

    private boolean sameGroup(DataType type, Object left, Object right, BlockHashKind kind) {
        BlockFactory factory = blockFactory;
        ElementType elementType = PlannerUtils.toElementType(type);
        try (Block block = pairBlock(factory, left, right)) {
            if (kind == BlockHashKind.ROW) {
                try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(elementType), factory)) {
                    int[] ids = hash.add(new Page(block));
                    return ids[0] == ids[1];
                }
            }
            BlockHash hash = kind == BlockHashKind.SINGLE
                ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024, false)
                : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024);
            int[][] ids = collect(hash, new Page(block), true);
            assertThat(ids[0].length, equalTo(1));
            assertThat(ids[1].length, equalTo(1));
            return ids[0][0] == ids[1][0];
        }
    }

    /** Every group id of every position. A {@code STATS} multivalue has more than one. */
    private static int[][] collect(BlockHash hash, Page page, boolean closeHash) {
        int count = page.getPositionCount();
        List<List<Integer>> found = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            found.add(new ArrayList<>());
        }
        try {
            hash.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    take(positionOffset, groupIds, found);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    take(positionOffset, groupIds, found);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int i = 0; i < groupIds.getPositionCount(); i++) {
                        found.get(positionOffset + i).add(groupIds.getInt(i));
                    }
                }

                @Override
                public void close() {}
            });
            int[][] ids = new int[count][];
            for (int i = 0; i < count; i++) {
                ids[i] = found.get(i).stream().mapToInt(Integer::intValue).toArray();
            }
            return ids;
        } finally {
            if (closeHash) {
                hash.close();
            }
        }
    }

    private static void take(int positionOffset, IntBlock groupIds, List<List<Integer>> found) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            if (groupIds.isNull(p)) {
                continue;
            }
            int start = groupIds.getFirstValueIndex(p);
            int values = groupIds.getValueCount(p);
            for (int v = 0; v < values; v++) {
                found.get(positionOffset + p).add(groupIds.getInt(start + v));
            }
        }
    }

    /**
     * Positions are {@code [1, 2]}, {@code [2, 1]}, {@code 1}, {@code 2}, {@code [1, 1]}. {@code STATS} splits the
     * lists. The repeated {@code 1} is one group.
     */
    private static Block intMultivalue(BlockFactory factory) {
        return factory.newIntBlockBuilder(6)
            .beginPositionEntry()
            .appendInt(1)
            .appendInt(2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendInt(2)
            .appendInt(1)
            .endPositionEntry()
            .appendInt(1)
            .appendInt(2)
            .beginPositionEntry()
            .appendInt(1)
            .appendInt(1)
            .endPositionEntry()
            .beginPositionEntry()
            .appendInt(1)
            .appendInt(2)
            .endPositionEntry()
            .build();
    }

    /** The byte-key counterpart of {@link #intMultivalue}. */
    private static Block keywordMultivalue(BlockFactory factory) {
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        return factory.newBytesRefBlockBuilder(6)
            .beginPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(b)
            .endPositionEntry()
            .beginPositionEntry()
            .appendBytesRef(b)
            .appendBytesRef(a)
            .endPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(b)
            .beginPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(a)
            .endPositionEntry()
            .beginPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(b)
            .endPositionEntry()
            .build();
    }

    /**
     * The {@link #intMultivalue} positions for every element type {@code STATS} splits. {@code a} is the first
     * distinct value and {@code b} the second.
     */
    private Block multivalue(ElementType elementType) {
        return switch (elementType) {
            case INT -> intMultivalue(blockFactory);
            case BYTES_REF -> keywordMultivalue(blockFactory);
            case LONG -> longMultivalue(blockFactory);
            case DOUBLE -> doubleMultivalue(blockFactory);
            case BOOLEAN -> booleanMultivalue(blockFactory);
            case DOUBLE_RANGE -> doubleRangeMultivalue(blockFactory);
            case FLOAT, NULL, DOC, COMPOSITE, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE ->
                throw new AssertionError("not a split multivalue [" + elementType + "]");
        };
    }

    private static Block longMultivalue(BlockFactory factory) {
        return factory.newLongBlockBuilder(6)
            .beginPositionEntry()
            .appendLong(1)
            .appendLong(2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendLong(2)
            .appendLong(1)
            .endPositionEntry()
            .appendLong(1)
            .appendLong(2)
            .beginPositionEntry()
            .appendLong(1)
            .appendLong(1)
            .endPositionEntry()
            .beginPositionEntry()
            .appendLong(1)
            .appendLong(2)
            .endPositionEntry()
            .build();
    }

    private static Block doubleMultivalue(BlockFactory factory) {
        return factory.newDoubleBlockBuilder(6)
            .beginPositionEntry()
            .appendDouble(1.5)
            .appendDouble(2.5)
            .endPositionEntry()
            .beginPositionEntry()
            .appendDouble(2.5)
            .appendDouble(1.5)
            .endPositionEntry()
            .appendDouble(1.5)
            .appendDouble(2.5)
            .beginPositionEntry()
            .appendDouble(1.5)
            .appendDouble(1.5)
            .endPositionEntry()
            .beginPositionEntry()
            .appendDouble(1.5)
            .appendDouble(2.5)
            .endPositionEntry()
            .build();
    }

    private static Block booleanMultivalue(BlockFactory factory) {
        return factory.newBooleanBlockBuilder(6)
            .beginPositionEntry()
            .appendBoolean(false)
            .appendBoolean(true)
            .endPositionEntry()
            .beginPositionEntry()
            .appendBoolean(true)
            .appendBoolean(false)
            .endPositionEntry()
            .appendBoolean(false)
            .appendBoolean(true)
            .beginPositionEntry()
            .appendBoolean(false)
            .appendBoolean(false)
            .endPositionEntry()
            .beginPositionEntry()
            .appendBoolean(false)
            .appendBoolean(true)
            .endPositionEntry()
            .build();
    }

    private static Block doubleRangeMultivalue(BlockFactory factory) {
        return factory.newDoubleRangeBlockBuilder(6)
            .beginPositionEntry()
            .appendDoubleRange(1, 2)
            .appendDoubleRange(1, 3)
            .endPositionEntry()
            .beginPositionEntry()
            .appendDoubleRange(1, 3)
            .appendDoubleRange(1, 2)
            .endPositionEntry()
            .appendDoubleRange(1, 2)
            .appendDoubleRange(1, 3)
            .beginPositionEntry()
            .appendDoubleRange(1, 2)
            .appendDoubleRange(1, 2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendDoubleRange(1, 2)
            .appendDoubleRange(1, 3)
            .endPositionEntry()
            .build();
    }

    private void assertSplit(BlockHashKind kind, Block block) {
        ElementType elementType = block.elementType();
        BlockHash hash = kind == BlockHashKind.SINGLE
            ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024, false)
            : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024);
        int[][] ids = collect(hash, new Page(block), true);
        assertThat(set(ids[0]), equalTo(set(ids[2][0], ids[3][0])));
        assertThat(set(ids[1]), equalTo(set(ids[0])));
        assertThat(set(ids[5]), equalTo(set(ids[0])));
        assertThat(ids[2][0], not(equalTo(ids[3][0])));
        assertThat(ids[4].length, equalTo(1));
        assertThat(ids[4][0], equalTo(ids[2][0]));
    }

    private static void assertPartition(int[] ids, int... expected) {
        assertThat(ids.length, equalTo(expected.length));
        for (int i = 0; i < expected.length; i++) {
            for (int j = i + 1; j < expected.length; j++) {
                assertThat("positions " + i + " and " + j, ids[i] == ids[j], equalTo(expected[i] == expected[j]));
            }
        }
    }

    private void assertWhole(Block block) {
        try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(block.elementType()), blockFactory)) {
            int[] ids = hash.add(new Page(block));
            assertPartition(ids, 0, 1, 2, 3, 4, 0);
        }
    }

    /** {@code [1, 2]}, the same vector again, then {@code [2, 1]}. */
    private static Block floatVectors(BlockFactory factory) {
        return factory.newFloatBlockBuilder(6)
            .beginPositionEntry()
            .appendFloat(1)
            .appendFloat(2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendFloat(1)
            .appendFloat(2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendFloat(2)
            .appendFloat(1)
            .endPositionEntry()
            .build();
    }

    private void assertRoundTrip(String label, Block input, BlockHash hash) {
        try {
            int[][] ids = collect(hash, new Page(input), false);
            try (IntVector nonEmpty = hash.nonEmpty()) {
                Block[] grouped = hash.getKeys(nonEmpty);
                input.incRef();
                Page probe = new Page(input);
                try {
                    try (RowInTableLookup lookup = RowInTableLookup.build(blockFactory, grouped)) {
                        try (
                            ReleasableIterator<IntBlock> rows = lookup.lookup(probe, blockFactory, ByteSizeValue.ofMb(1));
                            IntBlock matches = rows.next()
                        ) {
                            assertThat(label, matches.getPositionCount(), equalTo(input.getPositionCount()));
                            for (int p = 0; p < matches.getPositionCount(); p++) {
                                assertFalse(label + " position " + p, matches.isNull(p));
                                Set<Integer> found = new HashSet<>();
                                int start = matches.getFirstValueIndex(p);
                                int count = matches.getValueCount(p);
                                for (int v = 0; v < count; v++) {
                                    found.add(nonEmpty.getInt(matches.getInt(start + v)));
                                }
                                assertThat(label + " position " + p, found, equalTo(set(ids[p])));
                            }
                            assertFalse(rows.hasNext());
                        }
                    }
                } finally {
                    probe.releaseBlocks();
                    Releasables.close(grouped);
                }
            }
        } finally {
            hash.close();
        }
    }

    /**
     * Three positions, the first value repeated, and not an ascending int run. Null is three nulls.
     * {@code _source} and {@code _tsid} are bytes.
     */
    private static Block repeatedKeyBlock(ElementType elementType, BlockFactory blockFactory) {
        return switch (elementType) {
            case NULL -> blockFactory.newConstantNullBlock(3);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(3).appendBoolean(false).appendBoolean(true).appendBoolean(false).build();
            case INT -> blockFactory.newIntBlockBuilder(3).appendInt(2).appendInt(1).appendInt(2).build();
            case LONG -> blockFactory.newLongBlockBuilder(3).appendLong(2L).appendLong(1L).appendLong(2L).build();
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(3).appendDouble(2.5).appendDouble(1.5).appendDouble(2.5).build();
            case BYTES_REF -> blockFactory.newBytesRefBlockBuilder(3)
                .appendBytesRef(new BytesRef("b"))
                .appendBytesRef(new BytesRef("a"))
                .appendBytesRef(new BytesRef("b"))
                .build();
            case DOUBLE_RANGE -> blockFactory.newDoubleRangeBlockBuilder(3)
                .appendDoubleRange(1, 3)
                .appendDoubleRange(1, 2)
                .appendDoubleRange(1, 3)
                .build();
            case FLOAT, DOC, COMPOSITE, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE ->
                throw new AssertionError("not an INLINE STATS key [" + elementType + "]");
        };
    }

    private static Block valueBlock(BlockFactory factory, Object value) {
        if (value == null) {
            return factory.newConstantNullBlock(1);
        }
        return BlockUtils.fromList(factory, List.of(List.of(value)))[0];
    }

    private static Block pairBlock(BlockFactory factory, Object left, Object right) {
        if (left == null && right == null) {
            return factory.newConstantNullBlock(2);
        }
        List<Object> leftRow = left == null ? Arrays.asList(new Object[] { null }) : List.of(left);
        List<Object> rightRow = right == null ? Arrays.asList(new Object[] { null }) : List.of(right);
        return BlockUtils.fromList(factory, List.of(leftRow, rightRow))[0];
    }

    private static Set<Integer> set(int... values) {
        Set<Integer> set = new HashSet<>();
        for (int value : values) {
            set.add(value);
        }
        return set;
    }

    /** Reads one channel of the page, so {@code ==} runs on the block rather than a folded literal. */
    private static final class ChannelValue extends Expression implements EvaluatorMapper {
        private final DataType dataType;
        private final int channel;

        private ChannelValue(DataType dataType, int channel) {
            this(Source.EMPTY, dataType, channel);
        }

        private ChannelValue(Source source, DataType dataType, int channel) {
            super(source, List.of());
            this.dataType = dataType;
            this.channel = channel;
        }

        @Override
        public DataType dataType() {
            return dataType;
        }

        @Override
        public Nullability nullable() {
            return Nullability.TRUE;
        }

        @Override
        protected NodeInfo<ChannelValue> info() {
            return NodeInfo.create(this, ChannelValue::new, dataType, channel);
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            return this;
        }

        @Override
        public String getWriteableName() {
            return "channel-for-test";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
            return new LoadFromPageEvaluator.Factory(channel);
        }
    }
}
