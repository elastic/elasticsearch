/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.BytesRefSwissHash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class LongBytesRefBlockHashTests extends BlockHashTestCase {

    public void testAdd() {
        try (LongBytesRefBlockHash hash = newHash()) {
            Set<BytesRef> rowBytes = new HashSet<>();
            int numPages = between(1, 5);
            for (int i = 0; i < numPages; i++) {
                int positionCount = between(1, 100);
                final BytesRefBlock bytes;
                if (randomBoolean()) {
                    bytes = randomOrdinalBytes(positionCount);
                } else {
                    bytes = randomBytes(positionCount);
                }
                try (bytes; LongBlock longs = randomLongs(positionCount)) {
                    addAndAssertGroupIds(hash, longs, bytes);
                    for (int p = 0; p < positionCount; p++) {
                        for (Key key : keysAt(longs, bytes, p)) {
                            if (key.bytes() != null) {
                                rowBytes.add(key.bytes());
                            }
                        }
                    }
                }
            }
            // dictionary entries no row references never enter the hash
            assertThat(hash.bytesHashSize(), equalTo((long) rowBytes.size()));
        }
    }

    public void testLookup() {
        try (LongBytesRefBlockHash hash = newHash()) {
            int numPages = between(1, 5);
            for (int i = 0; i < numPages; i++) {
                int positionCount = between(1, 100);
                final BytesRefBlock bytes;
                if (randomBoolean()) {
                    bytes = randomOrdinalBytes(positionCount);
                } else {
                    bytes = randomBytes(positionCount);
                }
                try (bytes; LongBlock longs = randomLongs(positionCount)) {
                    addAndAssertGroupIds(hash, longs, bytes);
                }
            }
            final int numKeys = hash.numKeys();
            final long bytesHashSize = hash.bytesHashSize();
            int numLookups = between(1, 10);
            for (int i = 0; i < numLookups; i++) {
                int positionCount = between(1, 20);
                final BytesRefBlock bytes;
                if (randomBoolean()) {
                    bytes = randomOrdinalBytes(positionCount);
                } else {
                    bytes = randomBytes(positionCount);
                }
                try (bytes; LongBlock longs = randomLongs(positionCount)) {
                    lookupAndAssertGroupIds(hash, longs, bytes);
                }
            }
            assertThat(hash.numKeys(), equalTo(numKeys));
            assertThat(hash.bytesHashSize(), equalTo(bytesHashSize));
        }
    }

    /**
     * A dense page whose dictionary has entries no position references, including ordinal 0: only the referenced
     * entries are hashed on add, and a lookup through the same dictionary adds nothing.
     */
    public void testDenseOrdinalsHashOnlyReferencedEntries() {
        List<String> dictionary = List.of("unused-0", "a", "b", "unused-3");
        int positionCount = 12;
        assertTrue(OrdinalBytesRefBlock.isDense(positionCount, dictionary.size()));
        try (LongBytesRefBlockHash hash = newHash()) {
            try (
                BytesRefBlock bytes = ordinalVector(positionCount, p -> 1 + p % 2, dictionary);
                LongBlock longs = blockFactory.newConstantLongBlockWith(7, positionCount)
            ) {
                addAndAssertGroupIds(hash, longs, bytes);
            }
            assertThat(hash.bytesHashSize(), equalTo(2L));
            assertThat(hash.numKeys(), equalTo(2));
            // a is known, unused-3 is not
            try (
                BytesRefBlock bytes = ordinalVector(positionCount, p -> p % 2 == 0 ? 1 : 3, dictionary);
                LongBlock longs = blockFactory.newConstantLongBlockWith(7, positionCount)
            ) {
                lookupAndAssertGroupIds(hash, longs, bytes);
            }
            assertThat(hash.ordinalLookups(), equalTo(1L));
            assertThat(hash.bytesHashSize(), equalTo(2L));
        }
    }

    /**
     * {@link OrdinalBytesRefBlock#keepMask} keeps the whole dictionary; a value dropped by the mask must not be hashed.
     */
    public void testMaskedDenseOrdinalsHashOnlyKeptEntries() {
        int positionCount = 12;
        try (
            LongBytesRefBlockHash hash = newHash();
            IntBlock.Builder ords = blockFactory.newIntBlockBuilder(positionCount);
            BooleanVector.Builder mask = blockFactory.newBooleanVectorFixedBuilder(positionCount)
        ) {
            // the dropped entry is ordinal 0, so a masked position read as its raw value slot would hash it
            for (int p = 0; p < positionCount; p++) {
                boolean keep = p != 3 && p != 7;
                ords.appendInt(keep ? 1 + p % 2 : 0);
                mask.appendBoolean(keep);
            }
            try (
                OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ords.build(), dictVector(List.of("dropped", "a", "b")));
                BooleanVector maskVector = mask.build();
                BytesRefBlock bytes = block.keepMask(maskVector);
                LongBlock longs = blockFactory.newConstantLongBlockWith(7, positionCount)
            ) {
                assertNotNull(bytes.asOrdinals());
                assertTrue(bytes.asOrdinals().isDense());
                addAndAssertGroupIds(hash, longs, bytes);
            }
            assertThat(hash.bytesHashSize(), equalTo(2L));
        }
    }

    /**
     * A page that is not dense but repeats a few entries, as a row group clustered by the key, hashes each of them once
     * and leaves the rest of the dictionary out.
     */
    public void testRepeatedSparseOrdinalsHashOnlyReferencedEntries() {
        List<String> dictionary = IntStream.range(0, 100).mapToObj(i -> "d-" + i).toList();
        int[] used = { 50, 0, 99, 3 };
        int positionCount = 40;
        assertFalse(OrdinalBytesRefBlock.isDense(positionCount, dictionary.size()));
        try (LongBytesRefBlockHash hash = newHash()) {
            final BytesRefBlock bytes;
            if (randomBoolean()) {
                bytes = ordinalVector(positionCount, p -> used[p % used.length], dictionary);
            } else {
                try (IntBlock.Builder ords = blockFactory.newIntBlockBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        if (p % 5 == 4) {
                            ords.appendNull();
                        } else {
                            ords.appendInt(used[p % used.length]);
                        }
                    }
                    bytes = new OrdinalBytesRefBlock(ords.build(), dictVector(dictionary));
                }
            }
            try (bytes; LongBlock longs = randomLongs(positionCount)) {
                assertTrue(takesOrdinalAddPath(bytes));
                addAndAssertGroupIds(hash, longs, bytes);
            }
            assertThat(hash.bytesHashSize(), equalTo((long) used.length));
        }
    }

    /**
     * Ordinal pages added to a hash past the prefetch threshold: a repeated, not dense page and a dense, partly
     * referenced page both hash their entries through the prefetching path, and only the entries they reference.
     */
    public void testOrdinalsOnPrefetchingHash() {
        BytesRefSwissHash.PREFETCH_THRESHOLD = between(1, 1024);
        int seeded = between(2_000, 4_000); // exceeds the small-core capacity so a big core (and the prefetch path) forms
        int added = between(1, 100);
        int unused = between(1, 100);
        List<String> dictionary = new ArrayList<>();
        IntStream.range(0, seeded).forEach(i -> dictionary.add("v" + i));
        IntStream.range(0, added).forEach(i -> dictionary.add("new-" + i));
        IntStream.range(0, unused).forEach(i -> dictionary.add("unused-" + i));
        try (LongBytesRefBlockHash hash = newHash()) {
            try (
                BytesRefBlock bytes = dictVector(dictionary.subList(0, seeded)).asBlock();
                LongBlock longs = blockFactory.newConstantLongBlockWith(7, seeded)
            ) {
                addAndAssertGroupIds(hash, longs, bytes);
            }
            assertThat(hash.bytesHashSize(), equalTo((long) seeded));

            int referencedCount = between(1, DictionaryOrdinals.EARLY_CHECK_VALUES / DictionaryOrdinals.MIN_AVERAGE_REPEATS);
            int stride = seeded / referencedCount;
            int sparsePositions = Math.max(DictionaryOrdinals.MIN_VALUES, referencedCount * between(4, 8));
            try (
                BytesRefBlock bytes = ordinalVector(sparsePositions, p -> (p % referencedCount) * stride, dictionary);
                LongBlock longs = blockFactory.newConstantLongBlockWith(7, sparsePositions)
            ) {
                assertFalse(bytes.asOrdinals().isDense());
                assertTrue(takesOrdinalAddPath(bytes));
                addAndAssertGroupIds(hash, longs, bytes);
            }
            assertThat(hash.bytesHashSize(), equalTo((long) seeded));

            int densePositions = 2 * dictionary.size();
            try (
                BytesRefBlock bytes = ordinalVector(densePositions, p -> p % (seeded + added), dictionary);
                LongBlock longs = blockFactory.newConstantLongBlockWith(7, densePositions)
            ) {
                assertTrue(bytes.asOrdinals().isDense());
                addAndAssertGroupIds(hash, longs, bytes);
            }
            assertThat(hash.bytesHashSize(), equalTo((long) seeded + added));
        }
    }

    public void testAddSplitsMultivaluedPositionAcrossChunks() {
        // each position has 3 longs x 3 bytes = 9 group ids; 256 = 28 * 9 + 4, so the first chunk ends after 4 of
        // position 28's group ids and the other 5 arrive in the next chunk
        int positionCount = 40;
        try (
            LongBytesRefBlockHash hash = newHash(256);
            LongBlock.Builder longsBuilder = blockFactory.newLongBlockBuilder(positionCount);
            BytesRefBlock.Builder bytesBuilder = blockFactory.newBytesRefBlockBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                longsBuilder.beginPositionEntry().appendLong(1).appendLong(2).appendLong(3).endPositionEntry();
                bytesBuilder.beginPositionEntry()
                    .appendBytesRef(new BytesRef("a"))
                    .appendBytesRef(new BytesRef("b"))
                    .appendBytesRef(new BytesRef("c"))
                    .endPositionEntry();
            }
            try (LongBlock longs = longsBuilder.build(); BytesRefBlock bytes = bytesBuilder.build()) {
                int chunks = addAndAssertGroupIds(hash, longs, bytes);
                assertThat(chunks, greaterThan(1));
                assertThat(hash.numKeys(), equalTo(9));
            }
        }
    }

    private static boolean hasDenseOrdinals(BytesRefBlock bytes) {
        OrdinalBytesRefBlock ordinals = bytes.asOrdinals();
        return ordinals != null && ordinals.isDense();
    }

    /**
     * Whether adding {@code bytes} goes through the dictionary: dense pages always, other pages if they repeat their
     * values enough.
     */
    private static boolean takesOrdinalAddPath(BytesRefBlock bytes) {
        OrdinalBytesRefBlock ordinals = bytes.asOrdinals();
        return ordinals != null && (ordinals.isDense() || DictionaryOrdinals.distinct(ordinals.getOrdinalsBlock()) != null);
    }

    /**
     * Looks the page up and checks that every position resolves to exactly the groups of its known key combinations.
     */
    private static void lookupAndAssertGroupIds(LongBytesRefBlockHash hash, LongBlock longs, BytesRefBlock bytes) {
        final Map<Key, Integer> groups = groupsByKey(hash);
        long lookupsBefore = hash.ordinalLookups();
        int position = 0;
        try (ReleasableIterator<IntBlock> it = hash.lookup(new Page(longs, bytes), ByteSizeValue.ofKb(between(1, 16)))) {
            while (it.hasNext()) {
                try (IntBlock ords = it.next()) {
                    for (int p = 0; p < ords.getPositionCount(); p++, position++) {
                        // unknown combinations are simply absent; a position with none of them known is null
                        Set<Integer> expected = new HashSet<>();
                        for (Key key : keysAt(longs, bytes, position)) {
                            Integer groupId = groups.get(key);
                            if (groupId != null) {
                                expected.add(groupId);
                            }
                        }
                        assertThat("position " + position, groupIdsAt(ords, p), equalTo(expected));
                    }
                }
            }
        }
        assertThat(position, equalTo(longs.getPositionCount()));
        long expectedLookups = hasDenseOrdinals(bytes) ? lookupsBefore + 1 : lookupsBefore;
        assertThat(hash.ordinalLookups(), equalTo(expectedLookups));
    }

    private static void add(LongBytesRefBlockHash hash, boolean expectOrdinals, Consumer<OrdsAndKeys> callback, Block... values) {
        long before = hash.ordinalAdds();
        hash(true, hash, ordsAndKeys -> {
            long expectedAdds = expectOrdinals ? before + 1 : before;
            assertThat(hash.ordinalAdds(), equalTo(expectedAdds));
            assertThat(
                ordsAndKeys.description(),
                startsWith("LongBytesRefBlockHash{keys=[LongKey[channel=0], BytesRefKey[channel=1]], entries=" + hash.numKeys() + ", size=")
            );
            assertThat(
                ordsAndKeys.description(),
                endsWith("b, ordinalAdds=" + expectedAdds + ", ordinalLookups=" + hash.ordinalLookups() + "}")
            );
            callback.accept(ordsAndKeys);
        }, values);
    }

    record Key(Long longValue, BytesRef bytes) {}

    private static Set<Key> keysAt(LongBlock longs, BytesRefBlock bytes, int position) {
        List<Long> longValues = new ArrayList<>();
        int longStart = longs.getFirstValueIndex(position);
        for (int v = longStart; v < longStart + longs.getValueCount(position); v++) {
            longValues.add(longs.getLong(v));
        }
        if (longValues.isEmpty()) {
            longValues.add(null);
        }
        List<BytesRef> bytesValues = new ArrayList<>();
        int bytesStart = bytes.getFirstValueIndex(position);
        for (int v = bytesStart; v < bytesStart + bytes.getValueCount(position); v++) {
            bytesValues.add(BytesRef.deepCopyOf(bytes.getBytesRef(v, new BytesRef())));
        }
        if (bytesValues.isEmpty()) {
            bytesValues.add(null);
        }
        Set<Key> keys = new HashSet<>();
        for (Long l : longValues) {
            for (BytesRef b : bytesValues) {
                keys.add(new Key(l, b));
            }
        }
        return keys;
    }

    private static Set<Integer> groupIdsAt(IntBlock ords, int position) {
        Set<Integer> groupIds = new HashSet<>();
        int start = ords.getFirstValueIndex(position);
        for (int v = start; v < start + ords.getValueCount(position); v++) {
            groupIds.add(ords.getInt(v));
        }
        return groupIds;
    }

    private static Map<Key, Integer> groupsByKey(LongBytesRefBlockHash hash) {
        try (IntVector nonEmpty = hash.nonEmpty()) {
            Block[] keys = hash.getKeys(nonEmpty);
            try {
                LongBlock keyLongs = (LongBlock) keys[0];
                BytesRefBlock keyBytes = (BytesRefBlock) keys[1];
                Map<Key, Integer> groups = new HashMap<>();
                for (int i = 0; i < nonEmpty.getPositionCount(); i++) {
                    Set<Key> key = keysAt(keyLongs, keyBytes, i);
                    assertThat(key, hasSize(1));
                    groups.put(key.iterator().next(), nonEmpty.getInt(i));
                }
                return groups;
            } finally {
                Releasables.close(keys);
            }
        }
    }

    /**
     * Adds the page and checks that every position was collected into exactly the groups of its key combinations.
     * A chunk may end partway through a multivalued position, so group ids are collected across all chunks and
     * checked once the whole page has been added.
     * @return the number of emitted chunks
     */
    private static int addAndAssertGroupIds(LongBytesRefBlockHash hash, LongBlock longs, BytesRefBlock bytes) {
        Map<Integer, Set<Integer>> emitted = new HashMap<>();
        int[] chunks = new int[1];
        add(hash, takesOrdinalAddPath(bytes), ordsAndKeys -> {
            // only collect here: this chunk may end partway through a position
            chunks[0]++;
            IntBlock ords = ordsAndKeys.ords();
            for (int p = 0; p < ords.getPositionCount(); p++) {
                int position = ordsAndKeys.positionOffset() + p;
                Set<Integer> groupIds = emitted.computeIfAbsent(position, k -> new HashSet<>());
                int start = ords.getFirstValueIndex(p);
                for (int v = start; v < start + ords.getValueCount(p); v++) {
                    int groupId = ords.getInt(v);
                    assertTrue("group " + groupId + " emitted twice for position " + position, groupIds.add(groupId));
                }
            }
        }, longs, bytes);
        // every chunk has arrived, so each position now has all of its group ids
        Map<Key, Integer> groups = groupsByKey(hash);
        for (int position = 0; position < longs.getPositionCount(); position++) {
            Set<Integer> expected = new HashSet<>();
            for (Key key : keysAt(longs, bytes, position)) {
                Integer groupId = groups.get(key);
                assertNotNull("missing group for " + key, groupId);
                expected.add(groupId);
            }
            assertThat("position " + position, emitted.getOrDefault(position, Set.of()), equalTo(expected));
        }
        return chunks[0];
    }

    private LongBytesRefBlockHash newHash() {
        return newHash(between(128, 16 * 1024));
    }

    private LongBytesRefBlockHash newHash(int emitBatchSize) {
        return new LongBytesRefBlockHash(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
            blockFactory,
            emitBatchSize,
            false
        );
    }

    private LongBlock randomLongs(int positionCount) {
        if (randomBoolean()) {
            try (var builder = blockFactory.newLongVectorBuilder(positionCount)) {
                for (int i = 0; i < positionCount; i++) {
                    builder.appendLong(between(1, 100));
                }
                return builder.build().asBlock();
            }
        }
        try (var builder = blockFactory.newLongBlockBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                int values = between(0, 3);
                switch (values) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.appendLong(between(1, 100));
                    default -> {
                        builder.beginPositionEntry();
                        for (int v = 0; v < values; v++) {
                            builder.appendLong(between(1, 100));
                        }
                        builder.endPositionEntry();
                    }
                }

            }
            return builder.build();
        }
    }

    private BytesRefBlock randomBytes(int positionCount) {
        if (randomBoolean()) {
            try (var bytes = blockFactory.newBytesRefVectorBuilder(positionCount)) {
                for (int i = 0; i < positionCount; i++) {
                    bytes.appendBytesRef(new BytesRef("v-" + between(1, 10)));
                }
                return bytes.build().asBlock();
            }
        }
        try (BytesRefBlock.Builder bytes = blockFactory.newBytesRefBlockBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                int values = between(0, 3);
                switch (values) {
                    case 0 -> bytes.appendNull();
                    case 1 -> bytes.appendBytesRef(new BytesRef("v-" + between(1, 10)));
                    default -> {
                        bytes.beginPositionEntry();
                        for (int v = 0; v < values; v++) {
                            bytes.appendBytesRef(new BytesRef("v-" + between(1, 10)));
                        }
                        bytes.endPositionEntry();
                    }
                }

            }
            return bytes.build();
        }
    }

    private static final List<String> TERMS = IntStream.range(0, 9).mapToObj(i -> "v-" + i).toList();

    private BytesRefVector dictVector(Collection<String> values) {
        try (BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(values.size())) {
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            return builder.build();
        }
    }

    private BytesRefBlock ordinalVector(int positionCount, IntUnaryOperator ordAt, List<String> dictionary) {
        try (IntVector.Builder ords = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                ords.appendInt(ordAt.applyAsInt(p));
            }
            return new OrdinalBytesRefVector(ords.build(), dictVector(dictionary)).asBlock();
        }
    }

    /**
     * Random ordinal bytes. The dictionary may hold entries no position references, before and after the referenced
     * ones (as with a shared Parquet row-group dictionary), sometimes enough of them that the page is not dense.
     */
    private OrdinalBytesRefBlock randomOrdinalBytes(int positionCount) {
        if (randomBoolean()) {
            // constant ordinals; point at a random entry so the dictionary ord and the hash ord can differ
            List<String> dict = randomSubsetOf(between(1, 3), TERMS);
            IntVector ords = blockFactory.newConstantIntVector(between(0, dict.size() - 1), positionCount);
            return new OrdinalBytesRefVector(ords, dictVector(dict)).asBlock();
        }
        Map<String, Integer> dict = new LinkedHashMap<>();
        for (int i = randomBoolean() ? 0 : between(1, 3); i > 0; i--) {
            dict.put("unused-leading-" + i, dict.size());
        }
        Supplier<Integer> dictOrd = () -> {
            String term = randomFrom(TERMS);
            int idx = dict.getOrDefault(term, -1);
            if (idx == -1) {
                idx = dict.size();
                dict.put(term, idx);
            }
            return idx;
        };
        Supplier<Collection<String>> dictWithTrailing = () -> {
            int trailing = switch (between(0, 2)) {
                case 0 -> 0;
                case 1 -> between(1, 3);
                case 2 -> between(positionCount, 3 * positionCount);
                default -> throw new AssertionError("unexpected");
            };
            List<String> entries = new ArrayList<>(dict.keySet());
            for (int i = 0; i < trailing; i++) {
                entries.add("unused-trailing-" + i);
            }
            return entries;
        };
        if (randomBoolean()) {
            try (var ords = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    ords.appendInt(p, dictOrd.get());
                }
                return new OrdinalBytesRefVector(ords.build(), dictVector(dictWithTrailing.get())).asBlock();
            }
        }
        try (var ords = blockFactory.newIntBlockBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                int values = between(0, 3);
                switch (values) {
                    case 0 -> ords.appendNull();
                    case 1 -> ords.appendInt(dictOrd.get());
                    default -> {
                        ords.beginPositionEntry();
                        for (int v = 0; v < values; v++) {
                            ords.appendInt(dictOrd.get());
                        }
                        ords.endPositionEntry();
                    }
                }
            }
            return new OrdinalBytesRefBlock(ords.build(), dictVector(dictWithTrailing.get()));
        }
    }
}
