package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.LongUnaryOperator;

class GlobalOrdToSegmentAndSegmentOrd {
    static class Writer {
        private final OrdinalSequence.GroupWriter globalOrdToContainingSegmentGroupWriter;
        private final OrdinalSequence.GroupWriter globalOrdToContainingSegmentOrdGroupWriter;
        private final OrdinalSequence.Writer globalOrdToContainingSegment;
        private final OrdinalSequence.Writer globalOrdToContainingSegmentOrd;

        Writer(
            OrdinalSequence.GroupWriter globalOrdToContainingSegmentGroupWriter,
            OrdinalSequence.GroupWriter globalOrdToContainingSegmentOrdGroupWriter, // NOCOMMIT use just one file
            long maxGlobalOrd,
            int maxSegment,
            long maxEncodedSegmentOrd
        ) throws IOException {
            this.globalOrdToContainingSegmentGroupWriter = globalOrdToContainingSegmentGroupWriter;
            this.globalOrdToContainingSegmentOrdGroupWriter = globalOrdToContainingSegmentOrdGroupWriter;
            globalOrdToContainingSegment = globalOrdToContainingSegmentGroupWriter.directWriter(maxGlobalOrd, maxSegment);
            globalOrdToContainingSegmentOrd = globalOrdToContainingSegmentOrdGroupWriter.negativeDeltaWriter(
                maxGlobalOrd,
                maxEncodedSegmentOrd
            );
        }

        void write(long globalOrd, int containingSegment, long containingSegmentOrd) throws IOException {
            globalOrdToContainingSegment.add(globalOrd, containingSegment);
            globalOrdToContainingSegmentOrd.add(globalOrd, containingSegmentOrd);
        }

        ReaderProvider readerProvider() throws IOException {
            OrdinalSequence.GroupReader globalOrdToContainingSegmentGroupReader = null;
            OrdinalSequence.GroupReader globalOrdToContainingSegmentOrdGroupReader = null;
            try {
                globalOrdToContainingSegmentGroupReader = globalOrdToContainingSegmentGroupWriter.finish();
                globalOrdToContainingSegmentOrdGroupReader = globalOrdToContainingSegmentOrdGroupWriter.finish();
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

    static class ReaderProvider implements Accountable, Closeable {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReaderProvider.class);

        private final OrdinalSequence.GroupReader globalOrdToContainingSegmentGroupReader;
        private final OrdinalSequence.GroupReader globalOrdToContainingSegmentOrdGroupReader;
        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegment;
        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegmentOrd;

        ReaderProvider(
            OrdinalSequence.GroupReader globalOrdToContainingSegmentGroupReader,
            OrdinalSequence.GroupReader globalOrdToContainingSegmentOrdGroupReader,
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

        public long diskBytesUsed() throws IOException {
            return globalOrdToContainingSegmentGroupReader.diskBytesUsed() + globalOrdToContainingSegmentOrdGroupReader.diskBytesUsed();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(globalOrdToContainingSegmentGroupReader, globalOrdToContainingSegmentOrdGroupReader);
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
