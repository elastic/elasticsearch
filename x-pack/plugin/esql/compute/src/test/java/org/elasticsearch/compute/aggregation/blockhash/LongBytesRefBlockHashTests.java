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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class LongBytesRefBlockHashTests extends BlockHashTestCase {

    public void testAdd() {
        try (LongBytesRefBlockHash hash = newHash()) {
            int numPages = between(1, 5);
            for (int i = 0; i < numPages; i++) {
                int positionCount = between(1, 20);
                final BytesRefBlock bytes;
                if (randomBoolean()) {
                    bytes = randomOrdinalBytes(positionCount);
                } else {
                    bytes = randomBytes(positionCount);
                }
                try (bytes; LongBlock longs = randomLongs(positionCount)) {
                    add(hash, bytes.asOrdinals() != null, ordsAndKeys -> assertGroupIdsMatchKeys(ordsAndKeys, longs, bytes), longs, bytes);
                }
            }
        }
    }

    public void testLookup() {
        try (LongBytesRefBlockHash hash = newHash()) {
            int numPages = between(1, 5);
            for (int i = 0; i < numPages; i++) {
                int positionCount = between(1, 20);
                final BytesRefBlock bytes;
                if (randomBoolean()) {
                    bytes = randomOrdinalBytes(positionCount);
                } else {
                    bytes = randomBytes(positionCount);
                }
                try (bytes; LongBlock longs = randomLongs(positionCount)) {
                    add(hash, bytes.asOrdinals() != null, ordsAndKeys -> assertGroupIdsMatchKeys(ordsAndKeys, longs, bytes), longs, bytes);
                }
            }
            final int numKeys = hash.numKeys();
            final Map<Key, Integer> groups;
            try (IntVector nonEmpty = hash.nonEmpty()) {
                Block[] keys = hash.getKeys(nonEmpty);
                try {
                    groups = groupsByKey(nonEmpty, keys);
                } finally {
                    Releasables.close(keys);
                }
            }
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
                    assertThat(position, equalTo(positionCount));
                    long expectedLookups = bytes.asOrdinals() != null ? lookupsBefore + 1 : lookupsBefore;
                    assertThat(hash.ordinalLookups(), equalTo(expectedLookups));
                }
            }
            assertThat(hash.numKeys(), equalTo(numKeys));
        }
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

    private static Map<Key, Integer> groupsByKey(IntVector nonEmpty, Block[] keys) {
        LongBlock keyLongs = (LongBlock) keys[0];
        BytesRefBlock keyBytes = (BytesRefBlock) keys[1];
        Map<Key, Integer> groups = new HashMap<>();
        for (int i = 0; i < nonEmpty.getPositionCount(); i++) {
            Set<Key> key = keysAt(keyLongs, keyBytes, i);
            assertThat(key, hasSize(1));
            groups.put(key.iterator().next(), nonEmpty.getInt(i));
        }
        return groups;
    }

    private static void assertGroupIdsMatchKeys(OrdsAndKeys ordsAndKeys, LongBlock longs, BytesRefBlock bytes) {
        Map<Key, Integer> groups = groupsByKey(ordsAndKeys.nonEmpty(), ordsAndKeys.keys());
        IntBlock ords = ordsAndKeys.ords();
        for (int p = 0; p < ords.getPositionCount(); p++) {
            int position = ordsAndKeys.positionOffset() + p;
            Set<Integer> expected = new HashSet<>();
            for (Key key : keysAt(longs, bytes, position)) {
                Integer groupId = groups.get(key);
                assertNotNull("missing group for " + key, groupId);
                expected.add(groupId);
            }
            assertThat("position " + position, groupIdsAt(ords, p), equalTo(expected));
        }
    }

    private LongBytesRefBlockHash newHash() {
        return new LongBytesRefBlockHash(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
            blockFactory,
            between(128, 16 * 1024),
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

    private OrdinalBytesRefBlock randomOrdinalBytes(int positionCount) {
        if (randomBoolean()) {
            // constant ordinals; point at a random entry so the dictionary ord and the hash ord can differ
            List<String> dict = randomSubsetOf(between(1, 3), TERMS);
            IntVector ords = blockFactory.newConstantIntVector(between(0, dict.size() - 1), positionCount);
            return new OrdinalBytesRefVector(ords, dictVector(dict)).asBlock();
        }
        Map<String, Integer> dict = new LinkedHashMap<>();
        Supplier<Integer> dictOrd = () -> {
            String term = randomFrom(TERMS);
            int idx = dict.getOrDefault(term, -1);
            if (idx == -1) {
                idx = dict.size();
                dict.put(term, idx);
            }
            return idx;
        };
        if (randomBoolean()) {
            try (var ords = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    ords.appendInt(p, dictOrd.get());
                }
                return new OrdinalBytesRefVector(ords.build(), dictVector(dict.keySet())).asBlock();
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
            return new OrdinalBytesRefBlock(ords.build(), dictVector(dict.keySet()));
        }
    }
}
