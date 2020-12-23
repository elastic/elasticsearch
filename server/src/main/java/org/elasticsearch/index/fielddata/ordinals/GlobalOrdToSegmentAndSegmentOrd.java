package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

public class GlobalOrdToSegmentAndSegmentOrd {
    static class Writer {
        private final OrdinalSequence.Writer globalOrdToContainingSegment;
        private final OrdinalSequence.Writer globalOrdToContainingSegmentOrd;

        Writer(
            OrdinalSequence.OutHelper segmentOut,
            OrdinalSequence.OutHelper segmentOrdOut,
            long maxGlobalOrd,
            int maxSegment,
            long maxEncodedSegmentOrd
        ) throws IOException {
            globalOrdToContainingSegment = OrdinalSequence.directWriter(segmentOut, maxGlobalOrd, DirectWriter.bitsRequired(maxSegment));
            globalOrdToContainingSegmentOrd = OrdinalSequence.negativeDeltaWriter(segmentOrdOut, maxGlobalOrd, maxEncodedSegmentOrd);
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

        Reader get(RandomAccessInput segmentIn, RandomAccessInput segmentOrdIn) throws IOException {
            return new Reader(globalOrdToContainingSegment.get(segmentIn), globalOrdToContainingSegmentOrd.get(segmentOrdIn));
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
