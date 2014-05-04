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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 */
public class InternalGlobalOrdinalsBuilder extends AbstractIndexComponent implements GlobalOrdinalsBuilder {

    public final static int ORDINAL_MAPPING_THRESHOLD_DEFAULT = 2048;
    public final static String ORDINAL_MAPPING_THRESHOLD_KEY = "global_ordinals_compress_threshold";
    public final static String ORDINAL_MAPPING_THRESHOLD_INDEX_SETTING_KEY = "index." + ORDINAL_MAPPING_THRESHOLD_KEY;

    public InternalGlobalOrdinalsBuilder(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override
    public IndexFieldData.WithOrdinals build(final IndexReader indexReader, IndexFieldData.WithOrdinals indexFieldData, Settings settings, CircuitBreakerService breakerService) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTime = System.currentTimeMillis();

        // It makes sense to make the overhead ratio configurable for the mapping from segment ords to global ords
        // However, other mappings are never the bottleneck and only used to get the original value from an ord, so
        // it makes sense to force COMPACT for them
        final float acceptableOverheadRatio = settings.getAsFloat("acceptable_overhead_ratio", PackedInts.FAST);
        final AppendingPackedLongBuffer globalOrdToFirstSegment = new AppendingPackedLongBuffer(PackedInts.COMPACT);
        final MonotonicAppendingLongBuffer globalOrdToFirstSegmentDelta = new MonotonicAppendingLongBuffer(PackedInts.COMPACT);

        FieldDataType fieldDataType = indexFieldData.getFieldDataType();
        int defaultThreshold = settings.getAsInt(ORDINAL_MAPPING_THRESHOLD_INDEX_SETTING_KEY, ORDINAL_MAPPING_THRESHOLD_DEFAULT);
        int threshold = fieldDataType.getSettings().getAsInt(ORDINAL_MAPPING_THRESHOLD_KEY, defaultThreshold);
        OrdinalMappingSourceBuilder ordinalMappingBuilder = new OrdinalMappingSourceBuilder(
                indexReader.leaves().size(), acceptableOverheadRatio, threshold
        );

        long currentGlobalOrdinal = 0;
        final AtomicFieldData.WithOrdinals[] withOrdinals = new AtomicFieldData.WithOrdinals[indexReader.leaves().size()];
        TermIterator termIterator = new TermIterator(indexFieldData, indexReader.leaves(), withOrdinals);
        for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
            globalOrdToFirstSegment.add(termIterator.firstReaderIndex());
            long globalOrdinalDelta = currentGlobalOrdinal - termIterator.firstLocalOrdinal();
            globalOrdToFirstSegmentDelta.add(globalOrdinalDelta);
            for (TermIterator.LeafSource leafSource : termIterator.competitiveLeafs()) {
                ordinalMappingBuilder.onOrdinal(leafSource.context.ord, leafSource.tenum.ord(), currentGlobalOrdinal);
            }
            currentGlobalOrdinal++;
        }

        // ram used for the globalOrd to segmentOrd and segmentOrd to firstReaderIndex lookups
        long memorySizeInBytesCounter = 0;
        globalOrdToFirstSegment.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegment.ramBytesUsed();
        globalOrdToFirstSegmentDelta.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegmentDelta.ramBytesUsed();

        final long maxOrd = currentGlobalOrdinal;
        OrdinalMappingSource[] segmentOrdToGlobalOrdLookups = ordinalMappingBuilder.build(maxOrd);
        // add ram used for the main segmentOrd to globalOrd lookups
        memorySizeInBytesCounter += ordinalMappingBuilder.getMemorySizeInBytes();

        final long memorySizeInBytes = memorySizeInBytesCounter;
        breakerService.getBreaker().addWithoutBreaking(memorySizeInBytes);

        if (logger.isDebugEnabled()) {
            // this does include the [] from the array in the impl name
            String implName = segmentOrdToGlobalOrdLookups.getClass().getSimpleName();
            logger.debug(
                    "Global-ordinals[{}][{}][{}] took {} ms",
                    implName,
                    indexFieldData.getFieldNames().fullName(),
                    maxOrd,
                    (System.currentTimeMillis() - startTime)
            );
        }
        return new InternalGlobalOrdinalsIndexFieldData(indexFieldData.index(), settings, indexFieldData.getFieldNames(),
                fieldDataType, withOrdinals, globalOrdToFirstSegment, globalOrdToFirstSegmentDelta,
                segmentOrdToGlobalOrdLookups, memorySizeInBytes
        );
    }

    public interface OrdinalMappingSource {

        Ordinals.Docs globalOrdinals(Ordinals.Docs segmentOrdinals);

    }

    public static abstract class GlobalOrdinalMapping implements Ordinals.Docs {

        protected final Ordinals.Docs segmentOrdinals;
        private final long memorySizeInBytes;
        protected final long maxOrd;

        protected long currentGlobalOrd;

        private GlobalOrdinalMapping(Ordinals.Docs segmentOrdinals, long memorySizeInBytes, long maxOrd) {
            this.segmentOrdinals = segmentOrdinals;
            this.memorySizeInBytes = memorySizeInBytes;
            this.maxOrd = maxOrd;
        }

        @Override
        public final long getMaxOrd() {
            return maxOrd;
        }

        @Override
        public final boolean isMultiValued() {
            return segmentOrdinals.isMultiValued();
        }

        @Override
        public final int setDocument(int docId) {
            return segmentOrdinals.setDocument(docId);
        }

        @Override
        public final long currentOrd() {
            return currentGlobalOrd;
        }

        @Override
        public final long getOrd(int docId) {
            long segmentOrd = segmentOrdinals.getOrd(docId);
            if (segmentOrd == Ordinals.MISSING_ORDINAL) {
                return currentGlobalOrd = Ordinals.MISSING_ORDINAL;
            } else {
                return currentGlobalOrd = getGlobalOrd(segmentOrd);
            }
        }

        @Override
        public final long nextOrd() {
            long segmentOrd = segmentOrdinals.nextOrd();
            return currentGlobalOrd = getGlobalOrd(segmentOrd);
        }

        public abstract long getGlobalOrd(long segmentOrd);

    }

    private final static class OrdinalMappingSourceBuilder {

        final MonotonicAppendingLongBuffer[] segmentOrdToGlobalOrdDeltas;
        final float acceptableOverheadRatio;
        final int numSegments;
        final int threshold;

        long memorySizeInBytesCounter;

        private OrdinalMappingSourceBuilder(int numSegments, float acceptableOverheadRatio, int threshold) {
            segmentOrdToGlobalOrdDeltas = new MonotonicAppendingLongBuffer[numSegments];
            for (int i = 0; i < segmentOrdToGlobalOrdDeltas.length; i++) {
                segmentOrdToGlobalOrdDeltas[i] = new MonotonicAppendingLongBuffer(acceptableOverheadRatio);
            }
            this.numSegments = numSegments;
            this.acceptableOverheadRatio = acceptableOverheadRatio;
            this.threshold = threshold;
        }

        public void onOrdinal(int readerIndex, long segmentOrdinal, long globalOrdinal) {
            long delta = globalOrdinal - segmentOrdinal;
            segmentOrdToGlobalOrdDeltas[readerIndex].add(delta);
        }

        public OrdinalMappingSource[] build(long maxOrd) {
            // If we find out that there are less then predefined number of ordinals, it is better to put the the
            // segment ordinal to global ordinal mapping in a packed ints, since the amount values are small and
            // will most likely fit in the CPU caches and MonotonicAppendingLongBuffer's compression will just be
            // unnecessary.

            if (maxOrd <= threshold) {
                // Rebuilding from MonotonicAppendingLongBuffer to PackedInts.Mutable is fast
                PackedInts.Mutable[] newSegmentOrdToGlobalOrdDeltas = new PackedInts.Mutable[numSegments];
                for (int i = 0; i < segmentOrdToGlobalOrdDeltas.length; i++) {
                    newSegmentOrdToGlobalOrdDeltas[i] = PackedInts.getMutable((int) segmentOrdToGlobalOrdDeltas[i].size(), PackedInts.bitsRequired(maxOrd), acceptableOverheadRatio);
                }

                for (int readerIndex = 0; readerIndex < segmentOrdToGlobalOrdDeltas.length; readerIndex++) {
                    MonotonicAppendingLongBuffer segmentOrdToGlobalOrdDelta = segmentOrdToGlobalOrdDeltas[readerIndex];

                    for (long ordIndex = 0; ordIndex < segmentOrdToGlobalOrdDelta.size(); ordIndex++) {
                        long ordDelta = segmentOrdToGlobalOrdDelta.get(ordIndex);
                        newSegmentOrdToGlobalOrdDeltas[readerIndex].set((int) ordIndex, ordDelta);
                    }
                }

                PackedIntOrdinalMappingSource[] sources = new PackedIntOrdinalMappingSource[numSegments];
                for (int i = 0; i < newSegmentOrdToGlobalOrdDeltas.length; i++) {
                    PackedInts.Reader segmentOrdToGlobalOrdDelta = newSegmentOrdToGlobalOrdDeltas[i];
                    if (segmentOrdToGlobalOrdDelta.size() == maxOrd) {
                        // This means that a segment contains all the value and in that case segment ordinals
                        // can be used as global ordinals. This will save an extra lookup per hit.
                        sources[i] = null;
                    } else {
                        long ramUsed = segmentOrdToGlobalOrdDelta.ramBytesUsed();
                        sources[i] = new PackedIntOrdinalMappingSource(segmentOrdToGlobalOrdDelta, ramUsed, maxOrd);
                        memorySizeInBytesCounter += ramUsed;
                    }

                }
                return sources;
            } else {
                OrdinalMappingSource[] sources = new OrdinalMappingSource[segmentOrdToGlobalOrdDeltas.length];
                for (int i = 0; i < segmentOrdToGlobalOrdDeltas.length; i++) {
                    MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdDeltas[i];
                    if (segmentOrdToGlobalOrdLookup.size() == maxOrd) {
                        // idem as above
                        sources[i] = null;
                    } else {
                        segmentOrdToGlobalOrdLookup.freeze();
                        long ramUsed = segmentOrdToGlobalOrdLookup.ramBytesUsed();
                        sources[i] = new CompressedOrdinalMappingSource(segmentOrdToGlobalOrdLookup, ramUsed, maxOrd);
                        memorySizeInBytesCounter += ramUsed;
                    }
                }
                return sources;
            }
        }

        public long getMemorySizeInBytes() {
            return memorySizeInBytesCounter;
        }
    }

    private final static class CompressedOrdinalMappingSource implements OrdinalMappingSource {

        private final MonotonicAppendingLongBuffer globalOrdinalMapping;
        private final long memorySizeInBytes;
        private final long maxOrd;

        private CompressedOrdinalMappingSource(MonotonicAppendingLongBuffer globalOrdinalMapping, long memorySizeInBytes, long maxOrd) {
            this.globalOrdinalMapping = globalOrdinalMapping;
            this.memorySizeInBytes = memorySizeInBytes;
            this.maxOrd = maxOrd;
        }

        @Override
        public Ordinals.Docs globalOrdinals(Ordinals.Docs segmentOrdinals) {
            return new GlobalOrdinalsDocs(segmentOrdinals, globalOrdinalMapping, memorySizeInBytes, maxOrd);
        }

        private final static class GlobalOrdinalsDocs extends GlobalOrdinalMapping {

            private final MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup;

            private GlobalOrdinalsDocs(Ordinals.Docs segmentOrdinals, MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup, long memorySizeInBytes, long maxOrd) {
                super(segmentOrdinals, memorySizeInBytes, maxOrd);
                this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            }

            @Override
            public long getGlobalOrd(long segmentOrd) {
                return segmentOrd + segmentOrdToGlobalOrdLookup.get(segmentOrd);
            }
        }

    }

    private static final class PackedIntOrdinalMappingSource implements OrdinalMappingSource {

        private final PackedInts.Reader segmentOrdToGlobalOrdLookup;
        private final long memorySizeInBytes;
        private final long maxOrd;

        private PackedIntOrdinalMappingSource(PackedInts.Reader segmentOrdToGlobalOrdLookup, long memorySizeInBytes, long maxOrd) {
            this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            this.memorySizeInBytes = memorySizeInBytes;
            this.maxOrd = maxOrd;
        }

        @Override
        public Ordinals.Docs globalOrdinals(Ordinals.Docs segmentOrdinals) {
            return new GlobalOrdinalsDocs(segmentOrdinals, memorySizeInBytes, maxOrd, segmentOrdToGlobalOrdLookup);
        }

        private final static class GlobalOrdinalsDocs extends GlobalOrdinalMapping {

            private final PackedInts.Reader segmentOrdToGlobalOrdLookup;

            private GlobalOrdinalsDocs(Ordinals.Docs segmentOrdinals, long memorySizeInBytes, long maxOrd, PackedInts.Reader segmentOrdToGlobalOrdLookup) {
                super(segmentOrdinals, memorySizeInBytes, maxOrd);
                this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            }

            @Override
            public long getGlobalOrd(long segmentOrd) {
                return segmentOrd + segmentOrdToGlobalOrdLookup.get((int) segmentOrd);
            }
        }

    }

    private final static class TermIterator implements BytesRefIterator {

        private final LeafSourceQueue sources;
        private final List<LeafSource> competitiveLeafs = new ArrayList<>();

        private TermIterator(IndexFieldData.WithOrdinals indexFieldData, List<AtomicReaderContext> leaves, AtomicFieldData.WithOrdinals[] withOrdinals) throws IOException {
            this.sources = new LeafSourceQueue(leaves.size());
            for (int i = 0; i < leaves.size(); i++) {
                AtomicReaderContext atomicReaderContext = leaves.get(i);
                AtomicFieldData.WithOrdinals afd = indexFieldData.load(atomicReaderContext);
                withOrdinals[i] = afd;
                LeafSource leafSource = new LeafSource(afd, atomicReaderContext);
                if (leafSource.current != null) {
                    sources.add(leafSource);
                }
            }
        }

        public BytesRef next() throws IOException {
            for (LeafSource top : competitiveLeafs) {
                if (top.next() != null) {
                    sources.add(top);
                }
            }
            competitiveLeafs.clear();
            if (sources.size() == 0) {
                return null;
            }

            do {
                LeafSource competitiveLeaf = sources.pop();
                competitiveLeafs.add(competitiveLeaf);
            } while (sources.size() > 0 && competitiveLeafs.get(0).current.equals(sources.top().current));
            return competitiveLeafs.get(0).current;
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }

        List<LeafSource> competitiveLeafs() throws IOException {
            return competitiveLeafs;
        }

        int firstReaderIndex() {
            return competitiveLeafs.get(0).context.ord;
        }

        long firstLocalOrdinal() throws IOException {
            return competitiveLeafs.get(0).tenum.ord();
        }

        private static class LeafSource {

            final TermsEnum tenum;
            final AtomicReaderContext context;

            BytesRef current;

            private LeafSource(AtomicFieldData.WithOrdinals afd, AtomicReaderContext context) throws IOException {
                this.tenum = afd.getTermsEnum();
                this.context = context;
                this.current = tenum.next();
            }

            BytesRef next() throws IOException {
                return current = tenum.next();
            }

        }

        private final static class LeafSourceQueue extends PriorityQueue<LeafSource> {

            private final Comparator<BytesRef> termComp = BytesRef.getUTF8SortedAsUnicodeComparator();

            LeafSourceQueue(int size) {
                super(size);
            }

            @Override
            protected boolean lessThan(LeafSource termsA, LeafSource termsB) {
                final int cmp = termComp.compare(termsA.current, termsB.current);
                if (cmp != 0) {
                    return cmp < 0;
                } else {
                    return termsA.context.ord < termsB.context.ord;
                }
            }
        }

    }
}
