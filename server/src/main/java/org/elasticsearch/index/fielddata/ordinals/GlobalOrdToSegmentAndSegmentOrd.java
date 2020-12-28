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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

class GlobalOrdToSegmentAndSegmentOrd {
    static class Writer {
        private final OrdinalSequence.Writer globalOrdToContainingSegment;
        private final OrdinalSequence.Writer globalOrdToContainingSegmentOrd;

        Writer(OrdinalSequence.GroupWriter groupWriter, long minGlobalOrd, long maxGlobalOrd) throws IOException {
            globalOrdToContainingSegment = groupWriter.nonNegativeWriter(minGlobalOrd, maxGlobalOrd);
            globalOrdToContainingSegmentOrd = groupWriter.negativeDeltaWriter(minGlobalOrd, maxGlobalOrd);
        }

        void write(long globalOrd, int containingSegment, long containingSegmentOrd) throws IOException {
            globalOrdToContainingSegment.add(globalOrd, containingSegment);
            globalOrdToContainingSegmentOrd.add(globalOrd, containingSegmentOrd);
        }

        ReaderProvider readerProvider() throws IOException {
            return new ReaderProvider(globalOrdToContainingSegment.readerProvider(), globalOrdToContainingSegmentOrd.readerProvider());
        }
    }

    static class ReaderProvider implements Accountable {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReaderProvider.class);

        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegment;
        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegmentOrd;

        ReaderProvider(
            OrdinalSequence.ReaderProvider globalOrdToContainingSegment,
            OrdinalSequence.ReaderProvider globalOrdToContainingSegmentOrd
        ) {
            this.globalOrdToContainingSegment = globalOrdToContainingSegment;
            this.globalOrdToContainingSegmentOrd = globalOrdToContainingSegmentOrd;
        }

        Reader get(RandomAccessInput input) throws IOException {
            return new Reader(globalOrdToContainingSegment.get(input), globalOrdToContainingSegmentOrd.get(input));
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.alignObjectSize(
                BASE_RAM_BYTES_USED + globalOrdToContainingSegment.ramBytesUsed() + globalOrdToContainingSegmentOrd.ramBytesUsed()
            );
        }
    }

    static class Reader {
        private final LongUnaryOperator globalOrdToContainingSegment;
        private final LongUnaryOperator globalOrdToContainingSegmentOrd;

        Reader(LongUnaryOperator globalOrdToContainingSegment, LongUnaryOperator globalOrdToContainingSegmentOrd) {
            this.globalOrdToContainingSegment = globalOrdToContainingSegment;
            this.globalOrdToContainingSegmentOrd = globalOrdToContainingSegmentOrd;
        }

        public int containingSegment(long globalOrd) {
            return (int) globalOrdToContainingSegment.applyAsLong(globalOrd);
        }

        public long containingSegmentOrd(long globalOrd) {
            return globalOrdToContainingSegmentOrd.applyAsLong(globalOrd);
        }
    }
}
