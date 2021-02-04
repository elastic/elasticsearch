/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.CoreMatchers.equalTo;

public class MultiSnapshotTests extends ESTestCase {

    public void testTrackSeqNoSimpleRange() throws Exception {
        final MultiSnapshot.SeqNoSet bitSet = new MultiSnapshot.SeqNoSet();
        final List<Long> values = LongStream.range(0, 1024).boxed().collect(Collectors.toList());
        Randomness.shuffle(values);
        for (int i = 0; i < 1023; i++) {
            assertThat(bitSet.getAndSet(values.get(i)), equalTo(false));
        }
        assertThat(bitSet.getAndSet(values.get(1023)), equalTo(false));
        assertThat(bitSet.getAndSet(between(0, 1023)), equalTo(true));
        assertThat(bitSet.getAndSet(between(1024, Integer.MAX_VALUE)), equalTo(false));
    }

    public void testTrackSeqNoDenseRanges() throws Exception {
        final MultiSnapshot.SeqNoSet bitSet = new MultiSnapshot.SeqNoSet();
        final LongSet normalSet = new LongHashSet();
        IntStream.range(0, scaledRandomIntBetween(5_000, 10_000)).forEach(i -> {
            long seq = between(0, 5000);
            boolean existed = normalSet.add(seq) == false;
            assertThat("SeqNoSet != Set" + seq, bitSet.getAndSet(seq), equalTo(existed));
        });
    }

    public void testTrackSeqNoSparseRanges() throws Exception {
        final MultiSnapshot.SeqNoSet bitSet = new MultiSnapshot.SeqNoSet();
        final LongSet normalSet = new LongHashSet();
        IntStream.range(0, scaledRandomIntBetween(5_000, 10_000)).forEach(i -> {
            long seq = between(i * 10_000, i * 30_000);
            boolean existed = normalSet.add(seq) == false;
            assertThat("SeqNoSet != Set", bitSet.getAndSet(seq), equalTo(existed));
        });
    }

    public void testTrackSeqNoMimicTranslogRanges() throws Exception {
        final MultiSnapshot.SeqNoSet bitSet = new MultiSnapshot.SeqNoSet();
        final LongSet normalSet = new LongHashSet();
        long currentSeq = between(10_000_000, 1_000_000_000);
        final int iterations = scaledRandomIntBetween(100, 2000);
        for (long i = 0; i < iterations; i++) {
            int batchSize = between(1, 1500);
            currentSeq -= batchSize;
            List<Long> batch = LongStream.range(currentSeq, currentSeq + batchSize)
                .boxed()
                .collect(Collectors.toList());
            Randomness.shuffle(batch);
            batch.forEach(seq -> {
                boolean existed = normalSet.add(seq) == false;
                assertThat("SeqNoSet != Set", bitSet.getAndSet(seq), equalTo(existed));
            });
        }
    }
}
