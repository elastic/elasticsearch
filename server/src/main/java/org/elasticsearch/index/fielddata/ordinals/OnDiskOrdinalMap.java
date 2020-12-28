package org.elasticsearch.index.fielddata.ordinals;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.BucketedSort.ExtraData;
import org.elasticsearch.search.sort.SortOrder;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongUnaryOperator;

public class OnDiskOrdinalMap implements Closeable, Accountable, IndexOrdinalsFieldData {
    public static final String FILE_PREFIX = "global_ords";

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OnDiskOrdinalMap.class);

    private static final Logger logger = LogManager.getLogger(OnDiskOrdinalMap.class);

    /**
     * The field data built at index time.
     */
    private final IndexOrdinalsFieldData indexed;

    /**
     * Map from segment's ordinal in the reader to an ordinal sorted by the
     * number of unique fields in the segment. We sort so that when we build
     * the global ord to segment ord map we tend to link back to the same
     * segment more frequently. *That* gives us better compression storing
     * the map and better disk locality looking up the actual bytes.
     */
    private final SortedSegments sortedSegments;

    /**
     * The number of global ordinals.
     */
    private final long globalOrdCount;
    /**
     * {@link #indexed}'s field data per segment.
     */
    private final LeafOrdinalsFieldData[] segmentFieldData;
    /**
     * Handle for the file containing the ordinal mappings.
     */
    private final OrdinalSequence.GroupReader groupReader;
    /**
     * Map from segment ordinal to global ordinal.
     */
    private final OrdinalSequence.ReaderProvider[] segmentOrdsToGlobalOrds;
    /**
     * "Reverse" global ordinal mapping.
     */
    private final GlobalOrdToSegmentAndSegmentOrd.ReaderProvider globalOrdsToSegments;

    public OnDiskOrdinalMap(Directory directory, IndexOrdinalsFieldData indexed, IndexReader reader, CircuitBreakerService breakerService)
        throws IOException {
        logger.trace("loading global ords for [{}]", indexed.getFieldName());

        this.indexed = indexed;

        long start = 0;
        if (logger.isDebugEnabled()) {
            start = System.nanoTime();
        }

        long minGlobalOrd = Integer.MAX_VALUE;
        long maxGlobalOrd = 0;
        segmentFieldData = new LeafOrdinalsFieldData[reader.leaves().size()];
        SortedSetDocValues[] values = new SortedSetDocValues[segmentFieldData.length];
        for (int segmentOrd = 0; segmentOrd < segmentFieldData.length; segmentOrd++) {
            segmentFieldData[segmentOrd] = indexed.load(reader.leaves().get(segmentOrd));
            values[segmentOrd] = segmentFieldData[segmentOrd].getOrdinalsValues();
            minGlobalOrd = Math.min(minGlobalOrd, values[segmentOrd].getValueCount());
            maxGlobalOrd += values[segmentOrd].getValueCount();
        }
        this.sortedSegments = new SortedSegments(values);
        boolean success = false;

        try {
            try (OrdinalSequence.GroupWriter groupWriter = OrdinalSequence.GroupWriter.tmpFile(FILE_PREFIX, "map", directory)) {
                SegmentState[] states = new SegmentState[values.length];
                PriorityQueue<SegmentState> queue = new PriorityQueue<SegmentState>(values.length) {
                    @Override
                    protected boolean lessThan(SegmentState a, SegmentState b) {
                        return a.currentTerm.compareTo(b.currentTerm) < 0;
                    }
                };
                for (int segmentOrd = 0; segmentOrd < values.length; segmentOrd++) {
                    SegmentState state = new SegmentState(groupWriter, sortedSegments.ordToSorted[segmentOrd], values[segmentOrd]);
                    states[segmentOrd] = state;
                    if (state.next() != null) {
                        queue.add(state);
                    }
                }
                GlobalOrdToSegmentAndSegmentOrd.Writer globalOrdsToSegmentsWriter = new GlobalOrdToSegmentAndSegmentOrd.Writer(
                    groupWriter,
                    minGlobalOrd,
                    maxGlobalOrd
                );

                BytesRefBuilder scratch = new BytesRefBuilder();
                long globalOrd = 0;

                while (queue.size() != 0) {
                    scratch.copyBytes(queue.top().currentTerm);
                    int minContainingSegment = Integer.MAX_VALUE;
                    long minContainingSegmentOrd = Integer.MAX_VALUE;
                    do {
                        SegmentState top = queue.top();
                        long segmentOrd = top.termsEnum.ord();
                        top.writer.add(segmentOrd, globalOrd);
                        if (top.sortedSegmentOrd < minContainingSegment) {
                            minContainingSegment = top.sortedSegmentOrd;
                            minContainingSegmentOrd = segmentOrd;
                        }

                        if (top.next() == null) {
                            queue.pop();
                            if (queue.size() == 0) {
                                break;
                            }
                        } else {
                            queue.updateTop();
                        }
                    } while (queue.top().currentTerm.equals(scratch.get()));
                    globalOrdsToSegmentsWriter.write(globalOrd, minContainingSegment, minContainingSegmentOrd);
                    globalOrd++;
                }

                groupReader = groupWriter.finish();
                globalOrdCount = globalOrd;
                segmentOrdsToGlobalOrds = new OrdinalSequence.ReaderProvider[states.length];
                for (int segmentOrd = 0; segmentOrd < values.length; segmentOrd++) {
                    segmentOrdsToGlobalOrds[segmentOrd] = states[segmentOrd].writer.readerProvider();
                }
                globalOrdsToSegments = globalOrdsToSegmentsWriter.readerProvider();
            }

            breakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(ramBytesUsed());
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "loaded global ords for [{}] with [{}] values using [{}] on heap and [{}] on disk in [{}]",
                    indexed.getFieldName(),
                    maxGlobalOrd + 1,
                    ByteSizeValue.ofBytes(ramBytesUsed()),
                    ByteSizeValue.ofBytes(diskBytesUsed()),
                    TimeValue.timeValueNanos(System.nanoTime() - start).toHumanReadableString(3)
                );
            }
            success = true;
        } finally {
            if (false == success) {
                logger.debug("failed to load global ords for [{}]", indexed.getFieldName());
                close();
            }
        }
    }

    private static class SegmentState {
        private final int sortedSegmentOrd;
        private final TermsEnum termsEnum;
        private final OrdinalSequence.Writer writer;
        private BytesRef currentTerm;

        SegmentState(OrdinalSequence.GroupWriter groupWriter, int sortedSegmentOrd, SortedSetDocValues values) throws IOException {
            this.sortedSegmentOrd = sortedSegmentOrd;
            termsEnum = values.termsEnum();
            writer = groupWriter.positiveDeltaWriter(values.getValueCount());
        }

        BytesRef next() throws IOException {
            currentTerm = termsEnum.next();
            return currentTerm;
        }
    }

    @Override
    public String getFieldName() {
        return indexed.getFieldName();
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return indexed.getValuesSourceType();
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        throw new UnsupportedOperationException();
    }

    public IndexOrdinalsFieldData fork() throws IOException {
        return new IndexOrdinalsFieldData() {
            private final RandomAccessInput input = groupReader.input();
            private final TermsEnum[] lookups = new TermsEnum[segmentOrdsToGlobalOrds.length];
            private GlobalOrdToSegmentAndSegmentOrd.Reader globalOrdToContainingSegment;

            private BytesRef sharedLookupOrd(long globalOrd) throws IOException {
                GlobalOrdToSegmentAndSegmentOrd.Reader globalOrdToContainingSegment = globalOrdToContainingSegment();
                int containingSegment = sortedSegments.sortedToOrd[globalOrdToContainingSegment.containingSegment(globalOrd)];
                TermsEnum lookup = lookup(containingSegment);
                lookup.seekExact(globalOrdToContainingSegment.containingSegmentOrd(globalOrd));
                return lookup.term();
            }

            private GlobalOrdToSegmentAndSegmentOrd.Reader globalOrdToContainingSegment() throws IOException {
                if (globalOrdToContainingSegment == null) {
                    globalOrdToContainingSegment = globalOrdsToSegments.get(input);
                }
                return globalOrdToContainingSegment;
            }

            private TermsEnum lookup(int segmentOrd) throws IOException {
                if (lookups[segmentOrd] == null) {
                    lookups[segmentOrd] = segmentFieldData[segmentOrd].getOrdinalsValues().termsEnum();
                }
                return lookups[segmentOrd];
            }

            @Override
            public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BucketedSort newBucketedSort(
                BigArrays bigArrays,
                Object missingValue,
                MultiValueMode sortMode,
                Nested nested,
                SortOrder sortOrder,
                DocValueFormat format,
                int bucketSize,
                ExtraData extra
            ) {
                throw new UnsupportedOperationException();
            }

            @Override
            public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
                return load(context);
            }

            @Override
            public LeafOrdinalsFieldData load(LeafReaderContext context) {
                LongUnaryOperator segmentToGlobal;
                try {
                    segmentToGlobal = segmentOrdsToGlobalOrds[context.ord].get(input);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                LeafOrdinalsFieldData segmentOrdsFieldData = indexed.load(context);
                return new LeafOrdinalsFieldData() {
                    @Override
                    public long ramBytesUsed() {
                        long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                        size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + segmentOrdsFieldData.ramBytesUsed();
                        return RamUsageEstimator.alignObjectSize(size);
                    }

                    @Override
                    public ScriptDocValues<?> getScriptValues() {
                        return segmentOrdsFieldData.getScriptValues();
                    }

                    @Override
                    public SortedBinaryDocValues getBytesValues() {
                        return segmentOrdsFieldData.getBytesValues();
                    }

                    @Override
                    public void close() {}

                    @Override
                    public SortedSetDocValues getOrdinalsValues() {
                        SortedSetDocValues segmentOrds = segmentOrdsFieldData.getOrdinalsValues();
                        return new SortedSetDocValues() {
                            @Override
                            public int nextDoc() throws IOException {
                                return segmentOrds.nextDoc();
                            }

                            @Override
                            public int docID() {
                                return segmentOrds.docID();
                            }

                            @Override
                            public long cost() {
                                return segmentOrds.cost();
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                return segmentOrds.advance(target);
                            }

                            @Override
                            public boolean advanceExact(int target) throws IOException {
                                return segmentOrds.advanceExact(target);
                            }

                            @Override
                            public long nextOrd() throws IOException {
                                long segmentOrd = segmentOrds.nextOrd();
                                if (segmentOrd == SortedSetDocValues.NO_MORE_ORDS) {
                                    return SortedSetDocValues.NO_MORE_ORDS;
                                } else {
                                    return segmentToGlobal.applyAsLong(segmentOrd);
                                }
                            }

                            @Override
                            public BytesRef lookupOrd(long ord) throws IOException {
                                return sharedLookupOrd(ord);
                            }

                            @Override
                            public long getValueCount() {
                                return globalOrdCount;
                            }
                        };
                    }
                };
            }

            @Override
            public ValuesSourceType getValuesSourceType() {
                return indexed.getValuesSourceType();
            }

            @Override
            public String getFieldName() {
                return indexed.getFieldName();
            }

            @Override
            public boolean supportsGlobalOrdinalsMapping() {
                return true;
            }

            @Override
            public LongUnaryOperator getOrdinalMapping(LeafReaderContext context) {
                try {
                    return segmentOrdsToGlobalOrds[context.ord].get(input);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public OrdinalMap getOrdinalMap() {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        throw new UnsupportedOperationException("call fork first");
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        throw new UnsupportedOperationException("call fork first");
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
        return this;
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        ExtraData extra
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LongUnaryOperator getOrdinalMapping(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_RAM_BYTES_USED;
        size += sortedSegments.ramBytesUsed();
        size += RamUsageEstimator.sizeOf(segmentFieldData);
        /*
         * These things can't be null unless the ctor failed part way through.
         * But we have to handle when it *does* fail part way through so we
         * can call `close`. 
         */
        size += segmentOrdsToGlobalOrds == null ? 0 : RamUsageEstimator.sizeOf(segmentOrdsToGlobalOrds);
        size += globalOrdsToSegments == null ? 0 : globalOrdsToSegments.ramBytesUsed();
        return RamUsageEstimator.alignObjectSize(size);
    }

    public long diskBytesUsed() throws IOException {
        /*
         * groupReader can only be null if the ctor fails part of the way
         * through. But we have to handle that case.
         */
        return groupReader == null ? 0 : groupReader.diskBytesUsed();
    }

    @Override
    public void close() throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "closing global ords for [{}] with [{}] values releasing [{}] on heap and [{}] on disk",
                indexed.getFieldName(),
                globalOrdCount,
                ByteSizeValue.ofBytes(ramBytesUsed()),
                ByteSizeValue.ofBytes(diskBytesUsed())
            );
        }
        List<Closeable> all = new ArrayList<>();
        Collections.addAll(all, segmentFieldData);
        all.add(groupReader);
        IOUtils.close(all);
    }

    /**
     * Bi-directional mapping between the segment's ordinal and its position
     * when sorted by the number of terms.
     */
    private static class SortedSegments implements Accountable {
        private final int[] sortedToOrd;
        private final int[] ordToSorted;

        SortedSegments(SortedSetDocValues[] values) {
            sortedToOrd = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                sortedToOrd[i] = i;
            }
            new InPlaceMergeSorter() {
                @Override
                protected void swap(int i, int j) {
                    int tmp = sortedToOrd[i];
                    sortedToOrd[i] = sortedToOrd[j];
                    sortedToOrd[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    return Long.compare(values[sortedToOrd[j]].getValueCount(), values[sortedToOrd[i]].getValueCount());
                }
            }.sort(0, values.length);
            ordToSorted = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                assert i == 0 || values[sortedToOrd[i - 1]].getValueCount() >= values[sortedToOrd[i]].getValueCount();
                ordToSorted[sortedToOrd[i]] = i;
            }
        }

        @Override
        public long ramBytesUsed() {
            long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
            size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.sizeOf(sortedToOrd);
            size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.sizeOf(ordToSorted);
            return RamUsageEstimator.alignObjectSize(size);
        }
    }
}
