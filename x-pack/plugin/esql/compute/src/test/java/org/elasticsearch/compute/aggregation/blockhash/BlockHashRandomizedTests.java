/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeTests;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

//@TestLogging(value = "org.elasticsearch.compute:TRACE", reason = "debug")
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
public class BlockHashRandomizedTests extends ComputeTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        /*
         * Run with only `LONG` elements because we have some
         * optimizations that hit if you only have those.
         */
        new ParamsBuilder().allowedType(new Basic(ElementType.LONG)).groups(1, 2, 3, 4, 5, 10).add(params);
        /*
         * Run with only `BYTES_REF` elements because we have some
         * optimizations that hit if you only have those.
         */
        new ParamsBuilder().allowedType(new Basic(ElementType.BYTES_REF)).groups(1, 2, 3, 4, 5).add(params);
        /*
         * Run with only `BYTES_REF` elements in an OrdinalBytesRefBlock
         * because we have a few optimizations that use it.
         */
        new ParamsBuilder().allowedType(new Ordinals(10)).groups(1, 2, 3, 4, 5).add(params);
        /*
         * Run with only `LONG` and `BYTES_REF` elements because
         * we have some optimizations that hit if you only have those.
         */
        new ParamsBuilder().allowedType(new Basic(ElementType.LONG))
            .allowedType(new Basic(ElementType.BYTES_REF))
            .groups(1, 2, 3, 4, 5)
            .add(params);
        /*
         * Run with only `LONG` and `INT` elements because
         * we have some optimizations that hit if you only have those.
         */
        new ParamsBuilder().allowedType(new Basic(ElementType.LONG))
            .allowedType(new Basic(ElementType.INT))
            .groups(1, 2, 3, 4, 5, 10)
            .add(params);
        /*
         * Any random source.
         */
        ParamsBuilder builder = new ParamsBuilder().groups(1, 2, 3, 4, 5);
        for (ElementType t : MultivalueDedupeTests.supportedTypes()) {
            builder.allowedType(new Basic(t));
        }
        builder.allowedType(new Ordinals(10)).add(params);
        return params;
    }

    /**
     * The type of {@link Block} being tested.
     */
    interface Type {
        /**
         * The type of the {@link ElementType elements} in the {@link Block}.
         */
        ElementType elementType();

        /**
         * Build a random {@link Block}.
         */
        RandomBlock randomBlock(int positionCount, int maxValuesPerPosition, int dups);
    }

    private final boolean forcePackedHash;
    private final int groups;
    private final int maxValuesPerPosition;
    private final int dups;
    private final List<? extends Type> allowedTypes;

    public BlockHashRandomizedTests(
        @Name("forcePackedHash") boolean forcePackedHash,
        @Name("groups") int groups,
        @Name("maxValuesPerPosition") int maxValuesPerPosition,
        @Name("dups") int dups,
        @Name("allowedTypes") List<Type> allowedTypes
    ) {
        this.forcePackedHash = forcePackedHash;
        this.groups = groups;
        this.maxValuesPerPosition = maxValuesPerPosition;
        this.dups = dups;
        this.allowedTypes = allowedTypes;
    }

    public void test() {
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
        /*
         * We don't use MockBigArrays or MockBlockFactory here because it slows the
         * test down a ton. If the test is failing in sneaky ways you can use it, but
         * try not to commit it.
         */
        BigArrays bigArrays = new BigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService, CircuitBreaker.REQUEST);
        test(BlockFactory.builder(bigArrays).build());
    }

    public void testWithCranky() {
        try {
            test(crankyBlockFactory());
            logger.info("cranky let us finish!");
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void test(BlockFactory blockFactory) {
        /*
         * Expected ordinals for checking lookup. Skipped if we have more than 5 groups because
         * it'd be too expensive to calculate.
         */
        Map<List<Object>, IntHashSet> expectedOrds = groups > 5 ? null : new HashMap<>();

        List<Type> types = randomList(groups, groups, () -> randomFrom(allowedTypes));
        List<ElementType> elementTypes = randomElementTypes(types);
        RandomBlock[] randomBlocks = new RandomBlock[types.size()];
        Block[] blocks = new Block[types.size()];
        int pageCount = between(1, groups < 10 ? 10 : 5);
        int positionCount = randomPositionsPerPage(expectedOrds != null, pageCount, elementTypes);
        int emitBatchSize = randomFrom(100, 1_000, 10_000);
        try (BlockHash blockHash = newBlockHash(blockFactory, emitBatchSize, elementTypes)) {
            logger.info("checking {}", blockHash);
            /*
             * Only the long/long, long/bytes_ref, and bytes_ref/long implementations don't collect nulls.
             */
            Oracle oracle = new Oracle(
                forcePackedHash
                    || false == (elementTypes.equals(List.of(ElementType.LONG, ElementType.LONG))
                        || elementTypes.equals(List.of(ElementType.LONG, ElementType.BYTES_REF))
                        || elementTypes.equals(List.of(ElementType.BYTES_REF, ElementType.LONG)))
            );

            for (int p = 0; p < pageCount; p++) {
                for (int g = 0; g < blocks.length; g++) {
                    randomBlocks[g] = types.get(g).randomBlock(positionCount, maxValuesPerPosition, dups);
                    blocks[g] = randomBlocks[g].block();
                }
                oracle.add(randomBlocks);
                int[] batchCount = new int[1];
                // PackedValuesBlockHash always chunks but the normal single value ones don't
                boolean usingSingle = forcePackedHash == false && types.size() == 1;
                BlockHashTests.hash(false, blockHash, ordsAndKeys -> {
                    if (usingSingle == false) {
                        /*
                         * Either we chunk to emitBatchSize, or we emit one entry per input
                         * page (the vector/vector fast path). Page sizes are assumed safe, so
                         * emitting exactly positionCount in one shot is acceptable.
                         */
                        assertThat(
                            ordsAndKeys.ords().getTotalValueCount(),
                            either(lessThanOrEqualTo(emitBatchSize)).or(equalTo(positionCount))
                        );
                    }
                    batchCount[0]++;
                    if (expectedOrds != null) {
                        recordExpectedOrds(expectedOrds, blocks, ordsAndKeys);
                    }
                }, blocks);
                if (usingSingle) {
                    assertThat(batchCount[0], equalTo(1));
                }
            }

            try (BlockHashKeysTestHelper helper = new BlockHashKeysTestHelper(blockHash)) {
                helper.assertKeys(helper.nonEmpty, oracle.keys);
                // It's slow to check a lot of subsets
                int subsetsToCheck = groups < 10 ? 5 : 2;
                for (int i = 0; i < subsetsToCheck; i++) {
                    helper.assertRandomKeySubset();
                }
            }

            if (blockHash instanceof LongLongBlockHash == false
                && blockHash instanceof BytesRefLongBlockHash == false
                && blockHash instanceof BytesRef2BlockHash == false
                && blockHash instanceof BytesRef3BlockHash == false) {
                assertLookup(blockFactory, expectedOrds, types, blockHash, oracle);
            }
        }
        assertThat(blockFactory.breaker().getUsed(), is(0L));
    }

    /**
     * Pick a position count that keeps {@link Oracle} from growing too large.
     * {@link Oracle#add} builds the full cross-product of values across all
     * groups per position — up to {@code maxValuesPerPosition^groups} combinations —
     * and large element sizes (e.g. {@code BYTES_REF}) multiply the cost, so we
     * scale down using {@link #elementSize(ElementType)}.
     */
    private int randomPositionsPerPage(boolean trackGroupIds, int pageCount, List<ElementType> elementTypes) {
        long maxCombosPerPosition = (long) Math.pow(maxValuesPerPosition, groups);
        long totalElementSize = elementTypes.stream().mapToLong(BlockHashRandomizedTests::elementSize).sum();
        // Per-key cost: ArrayList wrapper + all elements + TreeMap.Entry
        long bytesPerOracleKey = 56 + totalElementSize + 40;
        // expectedOrds holds a second copy of every key when groups <= 5
        long bytesPerKey = trackGroupIds ? bytesPerOracleKey * 2 : bytesPerOracleKey;
        long targetBytes = ByteSizeValue.ofMb(200).getBytes();
        long maxPositions = targetBytes / (pageCount * maxCombosPerPosition * bytesPerKey);
        if (maxPositions >= 10_000) {
            return randomFrom(100, 1_000, 10_000);
        }
        if (maxPositions >= 1_000) {
            return randomFrom(100, 1_000);
        }
        return 100;
    }

    private static List<ElementType> randomElementTypes(List<Type> types) {
        return types.stream().map(Type::elementType).toList();
    }

    private static long elementSize(ElementType elementType) {
        return switch (elementType) {
            case LONG, DOUBLE -> 24;  // boxed Long/Double
            case INT -> 16;           // boxed Integer
            case BYTES_REF -> 48;     // BytesRef(24) + byte[](~24)
            default -> 0;             // BOOLEAN (interned singleton), NULL
        };
    }

    private BlockHash newBlockHash(BlockFactory blockFactory, int emitBatchSize, List<ElementType> types) {
        List<BlockHash.GroupSpec> specs = new ArrayList<>(types.size());
        for (int c = 0; c < types.size(); c++) {
            specs.add(new BlockHash.GroupSpec(c, types.get(c)));
        }
        return forcePackedHash
            ? new PackedValuesBlockHash(specs, blockFactory, emitBatchSize)
            : BlockHash.build(specs, blockFactory, emitBatchSize, true);
    }

    private static final int LOOKUP_POSITIONS = 1_000;

    private void assertLookup(
        BlockFactory blockFactory,
        Map<List<Object>, IntHashSet> expectedOrds,
        List<Type> types,
        BlockHash blockHash,
        Oracle oracle
    ) {
        Block.Builder[] builders = new Block.Builder[types.size()];
        try {
            for (int b = 0; b < builders.length; b++) {
                builders[b] = types.get(b).elementType().newBlockBuilder(LOOKUP_POSITIONS, blockFactory);
            }
            for (int p = 0; p < LOOKUP_POSITIONS; p++) {
                /*
                 * Pick a random key, about half the time one that's present.
                 * Note: if the universe of keys is small the randomKey method
                 * is quite likely to spit out a key in the oracle. That's fine
                 * so long as we have tests with a large universe too.
                 */
                List<Object> key = randomBoolean() ? randomKey(types) : randomFrom(oracle.keys);
                for (int b = 0; b < builders.length; b++) {
                    BlockTestUtils.append(builders[b], key.get(b));
                }
            }
            Block[] keyBlocks = Block.Builder.buildAll(builders);
            try {
                for (Block block : keyBlocks) {
                    assertThat(block.getPositionCount(), equalTo(LOOKUP_POSITIONS));
                }
                try (ReleasableIterator<IntBlock> lookup = blockHash.lookup(new Page(keyBlocks), ByteSizeValue.ofKb(between(1, 100)))) {
                    int positionOffset = 0;
                    while (lookup.hasNext()) {
                        try (IntBlock ords = lookup.next()) {
                            for (int p = 0; p < ords.getPositionCount(); p++) {
                                List<Object> key = readKey(keyBlocks, positionOffset + p);
                                if (oracle.keys.contains(key) == false) {
                                    assertTrue(ords.isNull(p));
                                    continue;
                                }
                                assertThat(ords.getValueCount(p), equalTo(1));
                                if (expectedOrds != null) {
                                    assertTrue(expectedOrds.get(key).contains(ords.getInt(ords.getFirstValueIndex(p))));
                                }
                            }
                            positionOffset += ords.getPositionCount();
                        }
                    }
                    assertThat(positionOffset, equalTo(LOOKUP_POSITIONS));
                }
            } finally {
                Releasables.closeExpectNoException(keyBlocks);
            }

        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    private static List<Object> readKey(Block[] keyBlocks, int position) {
        List<Object> key = new ArrayList<>(keyBlocks.length);
        for (Block block : keyBlocks) {
            assertThat(block.getValueCount(position), lessThan(2));
            List<Object> v = valuesAtPositions(block, position, position + 1).get(0);
            key.add(v == null ? null : v.get(0));
        }
        return key;
    }

    private void recordExpectedOrds(Map<List<Object>, IntHashSet> expectedOrds, Block[] keyBlocks, BlockHashTests.OrdsAndKeys ordsAndKeys) {
        long start = System.nanoTime();
        for (int p = 0; p < ordsAndKeys.ords().getPositionCount(); p++) {
            for (List<Object> key : readKeys(keyBlocks, p + ordsAndKeys.positionOffset())) {
                IntHashSet ords = expectedOrds.computeIfAbsent(key, k -> new IntHashSet(1));
                int firstOrd = ordsAndKeys.ords().getFirstValueIndex(p);
                int endOrd = ordsAndKeys.ords().getValueCount(p) + firstOrd;
                for (int i = firstOrd; i < endOrd; i++) {
                    ords.add(ordsAndKeys.ords().getInt(i));
                }
            }
        }
        logger.info("finished collecting ords {} {}", expectedOrds.size(), timeValueNanos(System.nanoTime() - start));
    }

    private static List<List<Object>> readKeys(Block[] keyBlocks, int position) {
        List<List<Object>> keys = new ArrayList<>();
        keys.add(List.of());
        for (Block block : keyBlocks) {
            List<Object> values = valuesAtPositions(block, position, position + 1).get(0);
            List<List<Object>> newKeys = new ArrayList<>();
            for (Object v : values == null ? Collections.singletonList(null) : values) {
                for (List<Object> k : keys) {
                    List<Object> newKey = new ArrayList<>(k);
                    newKey.add(v);
                    newKeys.add(newKey);
                }
            }
            keys = newKeys;
        }
        return keys.stream().distinct().toList();
    }

    private static class Oracle {
        private final NavigableSet<List<Object>> keys = new TreeSet<>(new BlockHashKeysTestHelper.KeyComparator());

        private final boolean collectsNullLongs;

        private Oracle(boolean collectsNullLongs) {
            this.collectsNullLongs = collectsNullLongs;
        }

        void add(RandomBlock[] randomBlocks) {
            for (int p = 0; p < randomBlocks[0].block().getPositionCount(); p++) {
                add(randomBlocks, p, List.of());
            }
        }

        void add(RandomBlock[] randomBlocks, int p, List<Object> key) {
            if (key.size() == randomBlocks.length) {
                keys.add(key);
                return;
            }
            RandomBlock block = randomBlocks[key.size()];
            List<Object> values = block.values().get(p);
            if (values == null) {
                if (block.block().elementType() != ElementType.LONG || collectsNullLongs) {
                    List<Object> newKey = new ArrayList<>(key);
                    newKey.add(null);
                    add(randomBlocks, p, newKey);
                }
                return;
            }
            for (Object v : values) {
                List<Object> newKey = new ArrayList<>(key);
                newKey.add(v);
                add(randomBlocks, p, newKey);
            }
        }
    }

    private static List<Object> randomKey(List<Type> types) {
        return types.stream().map(t -> randomKeyElement(t.elementType())).toList();
    }

    public static Object randomKeyElement(ElementType type) {
        return switch (type) {
            case INT -> randomInt();
            case LONG -> randomLong();
            case DOUBLE -> randomDouble();
            case BYTES_REF -> new BytesRef(randomAlphaOfLength(5));
            case BOOLEAN -> randomBoolean();
            case NULL -> null;
            default -> throw new IllegalArgumentException("unsupported element type [" + type + "]");
        };
    }

    private record Basic(ElementType elementType) implements Type {
        @Override
        public RandomBlock randomBlock(int positionCount, int maxValuesPerPosition, int dups) {
            return RandomBlock.randomBlock(
                elementType,
                positionCount,
                (elementType == ElementType.NULL) || randomBoolean(),
                1,
                maxValuesPerPosition,
                0,
                dups
            );
        }

    }

    private record Ordinals(int dictionarySize) implements Type {
        @Override
        public ElementType elementType() {
            return ElementType.BYTES_REF;
        }

        @Override
        public RandomBlock randomBlock(int positionCount, int maxValuesPerPosition, int dups) {
            Map<String, Integer> dictionary = new HashMap<>();
            Set<String> keys = dictionary(maxValuesPerPosition);
            List<List<Object>> values = new ArrayList<>(positionCount);
            try (
                IntBlock.Builder ordinals = TestBlockFactory.getNonBreakingInstance()
                    .newIntBlockBuilder(positionCount * maxValuesPerPosition);
                BytesRefVector.Builder bytes = TestBlockFactory.getNonBreakingInstance().newBytesRefVectorBuilder(maxValuesPerPosition);
            ) {
                for (int p = 0; p < positionCount; p++) {
                    int valueCount = between(1, maxValuesPerPosition);
                    int dupCount = between(0, dups);

                    List<Integer> ordsAtPosition = new ArrayList<>();
                    List<Object> valuesAtPosition = new ArrayList<>();
                    values.add(valuesAtPosition);
                    if (valueCount != 1 || dupCount != 0) {
                        ordinals.beginPositionEntry();
                    }
                    for (int v = 0; v < valueCount; v++) {
                        String key = randomFrom(keys);
                        int ordinal = dictionary.computeIfAbsent(key, k -> {
                            bytes.appendBytesRef(new BytesRef(k));
                            return dictionary.size();
                        });
                        valuesAtPosition.add(new BytesRef(key));
                        ordinals.appendInt(ordinal);
                        ordsAtPosition.add(ordinal);
                    }
                    for (int v = 0; v < dupCount; v++) {
                        ordinals.appendInt(randomFrom(ordsAtPosition));
                    }
                    if (valueCount != 1 || dupCount != 0) {
                        ordinals.endPositionEntry();
                    }
                }
                return new RandomBlock(values, new OrdinalBytesRefBlock(ordinals.build(), bytes.build()));
            }
        }

        private Set<String> dictionary(int maxValuesPerPosition) {
            int count = Math.max(dictionarySize, maxValuesPerPosition);
            Set<String> values = new HashSet<>();
            while (values.size() < count) {
                values.add(randomAlphaOfLength(5));
            }
            return values;
        }
    }

    private static class ParamsBuilder {
        private final List<Type> allowedTypes = new ArrayList<>();
        private final List<Integer> groups = new ArrayList<>();

        ParamsBuilder allowedType(Type t) {
            allowedTypes.add(t);
            return this;
        }

        ParamsBuilder allowedType(ElementType elementType) {
            return allowedType(new Basic(elementType));
        }

        ParamsBuilder groups(int... groups) {
            for (int g : groups) {
                this.groups.add(g);
            }
            return this;
        }

        void add(List<Object[]> params) {
            for (boolean forcePackedHash : new boolean[] { false, true }) {
                for (int g : groups) {
                    for (int maxValuesPerPosition : new int[] { 1, 3 }) {
                        for (int dups : new int[] { 0, 2 }) {
                            params.add(new Object[] { forcePackedHash, g, maxValuesPerPosition, dups, allowedTypes });
                        }
                    }
                }
            }
        }
    }
}
