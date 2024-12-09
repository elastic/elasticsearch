/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.MockBlockFactory;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TestBlockFactory;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeTests;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ListMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//@TestLogging(value = "org.elasticsearch.compute:TRACE", reason = "debug")
public class BlockHashRandomizedTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<List<? extends Type>> allowedTypesChoices = List.of(
            /*
             * Run with only `LONG` elements because we have some
             * optimizations that hit if you only have those.
             */
            List.of(new Basic(ElementType.LONG)),
            /*
             * Run with only `BYTES_REF` elements because we have some
             * optimizations that hit if you only have those.
             */
            List.of(new Basic(ElementType.BYTES_REF)),
            /*
             * Run with only `BYTES_REF` elements in an OrdinalBytesRefBlock
             * because we have a few optimizations that use it.
             */
            List.of(new Ordinals(10)),
            /*
             * Run with only `LONG` and `BYTES_REF` elements because
             * we have some optimizations that hit if you only have
             * those.
             */
            List.of(new Basic(ElementType.LONG), new Basic(ElementType.BYTES_REF)),
            /*
             * Any random source.
             */
            Stream.concat(Stream.of(new Ordinals(10)), MultivalueDedupeTests.supportedTypes().stream().map(Basic::new)).toList()
        );

        List<Object[]> params = new ArrayList<>();
        for (boolean forcePackedHash : new boolean[] { false, true }) {
            for (int groups : new int[] { 1, 2, 3, 4, 5, 10 }) {
                for (int maxValuesPerPosition : new int[] { 1, 3 }) {
                    for (int dups : new int[] { 0, 2 }) {
                        for (List<? extends Type> allowedTypes : allowedTypesChoices) {
                            params.add(new Object[] { forcePackedHash, groups, maxValuesPerPosition, dups, allowedTypes });
                        }
                    }
                }
            }
        }
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
        BasicBlockTests.RandomBlock randomBlock(int positionCount, int maxValuesPerPosition, int dups);
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
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
        test(new MockBlockFactory(breaker, bigArrays));
    }

    public void testWithCranky() {
        CircuitBreakerService service = new CrankyCircuitBreakerService();
        CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, service);
        try {
            test(new MockBlockFactory(breaker, bigArrays));
            logger.info("cranky let us finish!");
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void test(MockBlockFactory blockFactory) {
        List<Type> types = randomList(groups, groups, () -> randomFrom(allowedTypes));
        List<ElementType> elementTypes = types.stream().map(Type::elementType).toList();
        BasicBlockTests.RandomBlock[] randomBlocks = new BasicBlockTests.RandomBlock[types.size()];
        Block[] blocks = new Block[types.size()];
        int pageCount = between(1, groups < 10 ? 10 : 5);
        int positionCount = 100;
        int emitBatchSize = 100;
        try (BlockHash blockHash = newBlockHash(blockFactory, emitBatchSize, elementTypes)) {
            /*
             * Only the long/long, long/bytes_ref, and bytes_ref/long implementations don't collect nulls.
             */
            Oracle oracle = new Oracle(
                forcePackedHash
                    || false == (elementTypes.equals(List.of(ElementType.LONG, ElementType.LONG))
                        || elementTypes.equals(List.of(ElementType.LONG, ElementType.BYTES_REF))
                        || elementTypes.equals(List.of(ElementType.BYTES_REF, ElementType.LONG)))
            );
            /*
             * Expected ordinals for checking lookup. Skipped if we have more than 5 groups because
             * it'd be too expensive to calculate.
             */
            Map<List<Object>, Set<Integer>> expectedOrds = groups > 5 ? null : new HashMap<>();

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
                        assertThat(ordsAndKeys.ords().getTotalValueCount(), lessThanOrEqualTo(emitBatchSize));
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

            Block[] keyBlocks = blockHash.getKeys();
            try {
                Set<List<Object>> keys = new TreeSet<>(new KeyComparator());
                for (int p = 0; p < keyBlocks[0].getPositionCount(); p++) {
                    List<Object> key = new ArrayList<>(keyBlocks.length);
                    for (Block keyBlock : keyBlocks) {
                        if (keyBlock.isNull(p)) {
                            key.add(null);
                        } else {
                            key.add(BasicBlockTests.valuesAtPositions(keyBlock, p, p + 1).get(0).get(0));
                            assertThat(keyBlock.getValueCount(p), equalTo(1));
                        }
                    }
                    boolean contained = keys.add(key);
                    assertTrue(contained);
                }

                if (false == keys.equals(oracle.keys)) {
                    List<List<Object>> keyList = new ArrayList<>();
                    keyList.addAll(keys);
                    ListMatcher keyMatcher = matchesList();
                    for (List<Object> k : oracle.keys) {
                        keyMatcher = keyMatcher.item(k);
                    }
                    assertMap(keyList, keyMatcher);
                }

                if (blockHash instanceof LongLongBlockHash == false
                    && blockHash instanceof BytesRefLongBlockHash == false
                    && blockHash instanceof BytesRef2BlockHash == false
                    && blockHash instanceof BytesRef3BlockHash == false) {
                    assertLookup(blockFactory, expectedOrds, types, blockHash, oracle);
                }
            } finally {
                Releasables.closeExpectNoException(keyBlocks);
                blockFactory.ensureAllBlocksAreReleased();
            }
        }
        assertThat(blockFactory.breaker().getUsed(), is(0L));
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
        Map<List<Object>, Set<Integer>> expectedOrds,
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
                                    assertThat(ords.getInt(ords.getFirstValueIndex(p)), in(expectedOrds.get(key)));
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
            List<Object> v = BasicBlockTests.valuesAtPositions(block, position, position + 1).get(0);
            key.add(v == null ? null : v.get(0));
        }
        return key;
    }

    private void recordExpectedOrds(
        Map<List<Object>, Set<Integer>> expectedOrds,
        Block[] keyBlocks,
        BlockHashTests.OrdsAndKeys ordsAndKeys
    ) {
        long start = System.nanoTime();
        for (int p = 0; p < ordsAndKeys.ords().getPositionCount(); p++) {
            for (List<Object> key : readKeys(keyBlocks, p + ordsAndKeys.positionOffset())) {
                Set<Integer> ords = expectedOrds.computeIfAbsent(key, k -> new TreeSet<>());
                int firstOrd = ordsAndKeys.ords().getFirstValueIndex(p);
                int endOrd = ordsAndKeys.ords().getValueCount(p) + firstOrd;
                for (int i = firstOrd; i < endOrd; i++) {
                    ords.add(ordsAndKeys.ords().getInt(i));
                }
            }
        }
        logger.info("finished collecting ords {} {}", expectedOrds.size(), TimeValue.timeValueNanos(System.nanoTime() - start));
    }

    private static List<List<Object>> readKeys(Block[] keyBlocks, int position) {
        List<List<Object>> keys = new ArrayList<>();
        keys.add(List.of());
        for (Block block : keyBlocks) {
            List<Object> values = BasicBlockTests.valuesAtPositions(block, position, position + 1).get(0);
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

    static class KeyComparator implements Comparator<List<?>> {
        @Override
        public int compare(List<?> lhs, List<?> rhs) {
            for (int i = 0; i < lhs.size(); i++) {
                @SuppressWarnings("unchecked")
                Comparable<Object> l = (Comparable<Object>) lhs.get(i);
                Object r = rhs.get(i);
                if (l == null) {
                    if (r == null) {
                        continue;
                    } else {
                        return 1;
                    }
                }
                if (r == null) {
                    return -1;
                }
                int cmp = l.compareTo(r);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    }

    private static class Oracle {
        private final NavigableSet<List<Object>> keys = new TreeSet<>(new KeyComparator());

        private final boolean collectsNullLongs;

        private Oracle(boolean collectsNullLongs) {
            this.collectsNullLongs = collectsNullLongs;
        }

        void add(BasicBlockTests.RandomBlock[] randomBlocks) {
            for (int p = 0; p < randomBlocks[0].block().getPositionCount(); p++) {
                add(randomBlocks, p, List.of());
            }
        }

        void add(BasicBlockTests.RandomBlock[] randomBlocks, int p, List<Object> key) {
            if (key.size() == randomBlocks.length) {
                keys.add(key);
                return;
            }
            BasicBlockTests.RandomBlock block = randomBlocks[key.size()];
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

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
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
        public BasicBlockTests.RandomBlock randomBlock(int positionCount, int maxValuesPerPosition, int dups) {
            return BasicBlockTests.randomBlock(
                elementType,
                positionCount,
                elementType == ElementType.NULL | randomBoolean(),
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
        public BasicBlockTests.RandomBlock randomBlock(int positionCount, int maxValuesPerPosition, int dups) {
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
                return new BasicBlockTests.RandomBlock(values, new OrdinalBytesRefBlock(ordinals.build(), bytes.build()));
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
}
