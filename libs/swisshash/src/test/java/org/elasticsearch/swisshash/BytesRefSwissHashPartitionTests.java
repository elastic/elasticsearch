/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class BytesRefSwissHashPartitionTests extends PartitionedHashTestCase {

    private static void addInput(BytesRefSwissHash hash, SumAgg agg, BytesRef[] keys, int[] values) {
        final int[] ids = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            long id = hash.add(keys[i]);
            ids[i] = Math.toIntExact(id >= 0 ? id : -1 - id);
        }
        agg.ensureCapacity(Math.toIntExact(hash.size()));
        for (int v = 0; v < ids.length; v++) {
            agg.add(ids[v], values[v]);
        }
    }

    public void testPartition() {
        var recycler = new BytesRefSwissHashTests.TestRecycler();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(100)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        final int partitionSize = randomIntBetween(128, 10 * 1024);
        var hash1 = new BytesRefSwissHash(recycler, breaker, bigArrays);
        SumAgg agg1 = new SumAgg(breaker);
        var hash2 = new BytesRefSwissHash(recycler, breaker, bigArrays);
        SumAgg agg2 = new SumAgg(breaker);
        List<PartitionedKeyAndAggs> gens = new ArrayList<>();
        try {
            int numBlocks = between(1, 1000);
            for (int i = 0; i < numBlocks; i++) {
                final int positions = between(1, 2048);
                final BytesRef[] keys = new BytesRef[positions];
                boolean collisions = randomBoolean();
                for (int v = 0; v < positions; v++) {
                    final int len = collisions ? randomIntBetween(1, 5) : randomIntBetween(1, 20);
                    final byte[] bytes = new byte[len];
                    random().nextBytes(bytes);
                    keys[v] = new BytesRef(bytes);
                }
                final int[] values = new int[positions];
                for (int v = 0; v < positions; v++) {
                    values[v] = randomIntBetween(-1000, 1000);
                }
                addInput(hash1, agg1, keys, values);
                addInput(hash2, agg2, keys, values);

                if (hash2.size() >= partitionSize) {
                    gens.add(partition(breaker, hash2, hash2.size, agg2));
                    hash2.clear();
                    agg2.clear();
                }
            }
            if (hash2.size > 0) {
                gens.add(partition(breaker, hash2, hash2.size, agg2));
                hash2.clear();
                agg2.clear();
            }
            var result1 = emit(hash1, agg1);
            hash1.close();
            hash1 = null;
            agg1.close();
            agg1 = null;
            var results2 = combinePartitions(breaker, hash2, agg2, gens);
            assertThat(result1, equalTo(results2));
        } finally {
            Releasables.close(hash1, hash2, agg1, agg2);
            gens.forEach(g -> g.release(breaker));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    Set<KeyAndSum> combinePartitions(CircuitBreaker breaker, BytesRefSwissHash hash, SumAgg agg, List<PartitionedKeyAndAggs> gens) {
        int[] mergedIds = null;
        Set<KeyAndSum> results = new HashSet<>();
        for (int partition = 0; partition < PartitionedHashTable.NUM_PARTITIONS; partition++) {
            hash.clear();
            agg.clear();
            int totalKeys = 0;
            for (var gen : gens) {
                totalKeys += gen.keys().keysInPartition(partition);
            }
            agg.ensureCapacity(totalKeys);
            for (var gen : gens) {
                int numKeys = gen.keys().keysInPartition(partition);
                if (mergedIds == null || mergedIds.length < numKeys) {
                    mergedIds = new int[ArrayUtil.oversize(numKeys, Integer.BYTES)];
                }
                boolean appendOnly = hash.combinePartition(gen.keys(), partition, mergedIds);
                gen.keys().releasePartition(breaker, partition);
                agg.combinePartition(gen.aggs(), partition, mergedIds, numKeys, appendOnly);
                gen.aggs().releasePartition(breaker, partition);
            }
            for (var r : emit(hash, agg)) {
                if (results.add(r) == false) {
                    fail("key " + r.k1() + ":" + r.k2() + " already exists");
                }
            }
        }
        return results;
    }

    static Set<KeyAndSum> emit(BytesRefSwissHash hash, SumAgg agg) {
        Set<KeyAndSum> results = new HashSet<>(hash.size);
        BytesRef scratch = new BytesRef();
        for (int id = 0; id < hash.size; id++) {
            BytesRef key = hash.get(id, scratch);
            results.add(new KeyAndSum(BytesRefSwissHash.hash64(key), key.length, agg.sums[id]));
        }
        return results;
    }
}
