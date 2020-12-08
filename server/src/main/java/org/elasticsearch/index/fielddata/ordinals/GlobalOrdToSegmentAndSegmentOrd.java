package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.Closeable;
import java.io.IOException;

public class GlobalOrdToSegmentAndSegmentOrd {
    static class Writer implements Closeable {
        private final OrdinalSequence.Writer globalOrdToContainingSegment;
        private final OrdinalSequence.Writer globalOrdToContainingSegmentOrd;

        Writer(OrdinalSequence.IO segmentIO, OrdinalSequence.IO segmentOrdIO, long maxGlobalOrd, int maxSegment, long maxEncodedSegmentOrd)
            throws IOException {
            globalOrdToContainingSegment = OrdinalSequence.directWriter(segmentIO, maxGlobalOrd, DirectWriter.bitsRequired(maxSegment));
            globalOrdToContainingSegmentOrd = OrdinalSequence.negativeDeltaWriter(segmentOrdIO, maxGlobalOrd, maxEncodedSegmentOrd);
        }

        void write(long globalOrd, int containingSegment, long containingSegmentOrd) throws IOException {
            globalOrdToContainingSegment.add(globalOrd, containingSegment);
            globalOrdToContainingSegmentOrd.add(globalOrd, containingSegmentOrd);
        }

        ReaderProvider readerProvider() throws IOException {
            return new ReaderProvider(globalOrdToContainingSegment.readerProvider(), globalOrdToContainingSegmentOrd.readerProvider());
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(globalOrdToContainingSegment, globalOrdToContainingSegmentOrd);
        }
    }

    static class ReaderProvider implements Accountable, Closeable {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReaderProvider.class);

        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegment;
        private final OrdinalSequence.ReaderProvider globalOrdToContainingSegmentOrd;

        ReaderProvider(
            OrdinalSequence.ReaderProvider globalOrdToContainingSegment,
            OrdinalSequence.ReaderProvider globalOrdToContainingSegmentOrd
        ) {
            super();
            this.globalOrdToContainingSegment = globalOrdToContainingSegment;
            this.globalOrdToContainingSegmentOrd = globalOrdToContainingSegmentOrd;
        }

        Reader get() throws IOException {
            return new Reader(globalOrdToContainingSegment.get(), globalOrdToContainingSegmentOrd.get());
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.alignObjectSize(
                BASE_RAM_BYTES_USED + globalOrdToContainingSegment.ramBytesUsed() + globalOrdToContainingSegmentOrd.ramBytesUsed()
            );
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(globalOrdToContainingSegment, globalOrdToContainingSegmentOrd);
        }

        public long diskBytesUsed() throws IOException {
            return globalOrdToContainingSegment.diskBytesUsed() + globalOrdToContainingSegmentOrd.diskBytesUsed();
        }
    }

    static class Reader implements Closeable {
        private final OrdinalSequence.Reader globalOrdToContainingSegment;
        private final OrdinalSequence.Reader globalOrdToContainingSegmentOrd;

        Reader(OrdinalSequence.Reader globalOrdToContainingSegment, OrdinalSequence.Reader globalOrdToContainingSegmentOrd) {
            this.globalOrdToContainingSegment = globalOrdToContainingSegment;
            this.globalOrdToContainingSegmentOrd = globalOrdToContainingSegmentOrd;
        }

        public int containingSegment(long globalOrd) {
            return (int) globalOrdToContainingSegment.applyAsLong(globalOrd);
        }

        public long containingSegmentOrd(long globalOrd) {
            return globalOrdToContainingSegmentOrd.applyAsLong(globalOrd);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(globalOrdToContainingSegment, globalOrdToContainingSegmentOrd);
        }
    }

}
