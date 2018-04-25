/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
