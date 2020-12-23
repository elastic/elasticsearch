package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

class GlobalOrdToSegmentAndSegmentOrd {
    static class Writer {
        private final OrdinalSequence.Writer globalOrdToContainingSegment;
        private final OrdinalSequence.Writer globalOrdToContainingSegmentOrd;

        Writer(
            OrdinalSequence.GroupWriter segmentGWriter,
            OrdinalSequence.GroupWriter segmentOrdGWriter, // NOCOMMIT use just one file
            long maxGlobalOrd,
            int maxSegment,
            long maxEncodedSegmentOrd
        ) throws IOException {
            globalOrdToContainingSegment = segmentGWriter.directWriter(maxGlobalOrd, DirectWriter.bitsRequired(maxSegment));
            globalOrdToContainingSegmentOrd = segmentOrdGWriter.negativeDeltaWriter(maxGlobalOrd, maxEncodedSegmentOrd);
        }

        void write(long globalOrd, int containingSegment, long containingSegmentOrd) throws IOException {
            globalOrdToContainingSegment.add(globalOrd, containingSegment);
            globalOrdToContainingSegmentOrd.add(globalOrd, containingSegmentOrd);
        }

        ReaderProvider readerProvider() throws IOException {
            OrdinalSequence.InHelper globalOrdToContainingSegmentGroupReader;
            OrdinalSequence.InHelper globalOrdToContainingSegmentOrdGroupReader;
            try {
                globalOrdToContainingSegmentGroupReader = segmentGWriter.finish();
                globalOrdToContainingSegmentOrdGroupReader = segmentOrdGWriter.finish();
            } finally {
                if (globalOrdToContainingSegmentGroupReader == null || globalOrdToContainingSegmentOrdGroupReader == null) {
                    IOUtils.close(globalOrdToContainingSegmentGroupReader, globalOrdToContainingSegmentOrdGroupReader);
                }
            }
            return new ReaderProvider(
                globalOrdToContainingSegmentGroupReader,
                globalOrdToContainingSegmentOrdGroupReader,
                globalOrdToContainingSegment.readerProvider(),
                globalOrdToContainingSegmentOrd.readerProvider()
            );
        }
    }

    static class ReaderProvider implements Accountable {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReaderProvider.class);

        private final OrdinalSequence.InHelper globalOrdToContainingSegmentGroupReader;
        private final OrdinalSequence.InHelper globalOrdToContainingSegmentOrdGroupReader;
        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegment;
        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegmentOrd;

        ReaderProvider(
            OrdinalSequence.InHelper globalOrdToContainingSegmentGroupReader,
            OrdinalSequence.InHelper globalOrdToContainingSegmentOrdGroupReader,
            OrdinalSequence.ReaderProvider globalOrdToContainingSegment,
            OrdinalSequence.ReaderProvider globalOrdToContainingSegmentOrd
        ) {
            this.globalOrdToContainingSegmentGroupReader = globalOrdToContainingSegmentGroupReader;
            this.globalOrdToContainingSegmentOrdGroupReader = globalOrdToContainingSegmentOrdGroupReader;
            this.globalOrdToContainingSegment = globalOrdToContainingSegment;
            this.globalOrdToContainingSegmentOrd = globalOrdToContainingSegmentOrd;
        }

        Reader get() throws IOException {
            return new Reader(
                globalOrdToContainingSegment.get(globalOrdToContainingSegmentGroupReader.input()),
                globalOrdToContainingSegmentOrd.get(globalOrdToContainingSegmentOrdGroupReader.input())
            );
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
