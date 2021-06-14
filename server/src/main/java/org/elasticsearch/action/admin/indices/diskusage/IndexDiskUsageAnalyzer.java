/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.CountedBitSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Analyze the disk usage of each field in the index.
 */
public final class IndexDiskUsageAnalyzer implements Closeable {
    private final TrackingReadBytesDirectory directory;
    private final DirectoryReader directoryReader;
    private final CancellationChecker cancellationChecker;

    IndexDiskUsageAnalyzer(IndexCommit commit, Runnable checkForCancellation) throws IOException {
        this.directory = new TrackingReadBytesDirectory(commit.getDirectory());
        this.cancellationChecker = new CancellationChecker(checkForCancellation);
        this.directoryReader = DirectoryReader.open(new FilterIndexCommit(commit) {
            @Override
            public Directory getDirectory() {
                return directory;
            }
        });
    }

    public IndexDiskUsageStats analyze() throws IOException {
        final IndexDiskUsageStats stats = new IndexDiskUsageStats();
        for (LeafReaderContext leaf : directoryReader.leaves()) {
            cancellationChecker.checkForCancellation();
            final SegmentReader reader = Lucene.segmentReader(leaf.reader());
            analyzeStoredFields(reader, stats);
            analyzeDocValues(reader, stats);
            analyzePostings(reader, stats);
            analyzePoints(reader, stats);
            analyzeNorms(reader, stats);
            analyzeTermVectors(reader, stats);
        }
        return stats;
    }

    void analyzeStoredFields(SegmentReader reader, IndexDiskUsageStats stats) throws IOException {
        // We can't record the disk usages of stored fields in Lucene Codec as we need to record them for each chunk,
        // which is expensive; otherwise, bulk merge won't work.
        final StoredFieldsReader storedFieldsReader = reader.getFieldsReader().getMergeInstance();
        final TrackingSizeStoredFieldVisitor visitor = new TrackingSizeStoredFieldVisitor();
        directory.resetAndTrackBytesRead(BytesReadTrackingMode.SIZE_ONLY);
        for (int docID = 0; docID < reader.numDocs(); docID++) {
            cancellationChecker.logEvent();
            storedFieldsReader.visitDocument(docID, visitor);
        }
        if (visitor.fields.isEmpty() == false) {
            // Computing the compression ratio for each chunk would provide a better estimate for each field individually.
            // But it's okay to do this entire segment because source and _id are the only two stored fields in ES most the cases.
            final long totalBytes = visitor.fields.values().stream().mapToLong(v -> v).sum();
            final double ratio = (double) directory.getBytesRead() / (double) totalBytes;
            final FieldInfos fieldInfos = reader.getFieldInfos();
            for (Map.Entry<Integer, Long> field : visitor.fields.entrySet()) {
                final String fieldName = fieldInfos.fieldInfo(field.getKey()).name;
                final long fieldSize = (long) Math.ceil(field.getValue() * ratio);
                stats.addStoredField(fieldName, fieldSize);
            }
        }
    }

    private static class TrackingSizeStoredFieldVisitor extends StoredFieldVisitor {
        private final Map<Integer, Long> fields = new HashMap<>();

        private void trackField(FieldInfo fieldInfo, int bytes) {
            fields.compute(fieldInfo.number, (k, v) -> v == null ? bytes : v + bytes);
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
            trackField(fieldInfo, Integer.BYTES + value.length);
        }

        @Override
        public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
            trackField(fieldInfo, Integer.BYTES + value.length);
        }

        @Override
        public void intField(FieldInfo fieldInfo, int value) throws IOException {
            trackField(fieldInfo, Integer.BYTES);
        }

        @Override
        public void longField(FieldInfo fieldInfo, long value) throws IOException {
            trackField(fieldInfo, Long.BYTES);
        }

        @Override
        public void floatField(FieldInfo fieldInfo, float value) throws IOException {
            trackField(fieldInfo, Float.BYTES);
        }

        @Override
        public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
            trackField(fieldInfo, Double.BYTES);
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            return Status.YES;
        }
    }

    void analyzeDocValues(SegmentReader reader, IndexDiskUsageStats stats) throws IOException {
        // TODO: We can extract these stats from Lucene80DocValuesProducer without iterating all docValues
        // or track the new sliced IndexInputs.
        DocValuesProducer docValuesReader = reader.getDocValuesReader();
        if (docValuesReader == null) {
            return;
        }
        docValuesReader = docValuesReader.getMergeInstance();
        for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            final DocValuesType dvType = fieldInfo.getDocValuesType();
            if (dvType == DocValuesType.NONE) {
                continue;
            }
            cancellationChecker.checkForCancellation();
            directory.resetAndTrackBytesRead(BytesReadTrackingMode.SIZE_AND_POSITION);
            switch (dvType) {
                case NUMERIC:
                    final NumericDocValues numeric = docValuesReader.getNumeric(fieldInfo);
                    while (numeric.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        cancellationChecker.logEvent();
                        numeric.longValue();
                    }
                    break;
                case SORTED_NUMERIC:
                    final SortedNumericDocValues sortedNumeric = docValuesReader.getSortedNumeric(fieldInfo);
                    while (sortedNumeric.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        for (int i = 0; i < sortedNumeric.docValueCount(); i++) {
                            cancellationChecker.logEvent();
                            sortedNumeric.nextValue();
                        }
                    }
                    break;
                case BINARY:
                    final BinaryDocValues binary = docValuesReader.getBinary(fieldInfo);
                    while (binary.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        cancellationChecker.logEvent();
                        binary.binaryValue();
                    }
                    break;
                case SORTED:
                    final SortedDocValues sorted = docValuesReader.getSorted(fieldInfo);
                    while (sorted.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        cancellationChecker.logEvent();
                        sorted.ordValue();
                    }
                    for (int ord = 0; ord < sorted.getValueCount(); ord++) {
                        cancellationChecker.logEvent();
                        sorted.lookupOrd(ord);
                    }
                    break;
                case SORTED_SET:
                    final SortedSetDocValues sortedSet = docValuesReader.getSortedSet(fieldInfo);
                    while (sortedSet.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        cancellationChecker.logEvent();
                        while (sortedSet.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                            cancellationChecker.logEvent();
                        }
                    }
                    for (long ord = 0; ord < sortedSet.getValueCount(); ord++) {
                        cancellationChecker.logEvent();
                        sortedSet.lookupOrd(ord);
                    }
                    break;
                default:
                    assert false : "Unknown docValues type [" + dvType + "]";
                    throw new IllegalStateException("Unknown docValues type [" + dvType + "]");
            }
            final long bytesRead = directory.getBytesRead();
            if (bytesRead > 0) {
                stats.addDocValues(fieldInfo.name, bytesRead);
            }
        }
    }

    void analyzePostings(SegmentReader reader, IndexDiskUsageStats stats) throws IOException {
        // TODO: FieldsReader has stats() which might contain the disk usage infos
        // Also, can we track the byte reader per field extension to avoid visiting terms multiple times?
        FieldsProducer postingsReader = reader.getPostingsReader();
        if (postingsReader == null) {
            return;
        }
        postingsReader = postingsReader.getMergeInstance();
        PostingsEnum postings = null;
        for (FieldInfo field : reader.getFieldInfos()) {
            if (field.getIndexOptions() == IndexOptions.NONE) {
                continue;
            }
            cancellationChecker.checkForCancellation();
            directory.resetAndTrackBytesRead(BytesReadTrackingMode.SIZE_AND_POSITION);
            final Terms terms = postingsReader.terms(field.name);
            if (terms == null) {
                continue;
            }
            // Visit terms index and term dictionary
            final long termBytes;
            {
                final BytesRefIterator termsIterator = terms.iterator();
                final TermsEnum termsEnum = terms.iterator();
                BytesRef bytesRef;
                while ((bytesRef = termsIterator.next()) != null) {
                    cancellationChecker.logEvent();
                    final boolean found = termsEnum.seekExact(bytesRef);
                    assert found;
                }
                termBytes = directory.getBytesRead();
                if (termBytes > 0) {
                    stats.addTerms(field.name, termBytes);
                }
            }

            // Visit posting and skip lists
            final long postingsBytes;
            {
                final TermsEnum termsEnum = terms.iterator();
                while (termsEnum.next() != null) {
                    termsEnum.totalTermFreq();
                    postings = termsEnum.postings(postings, PostingsEnum.NONE);
                    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        cancellationChecker.logEvent();
                        postings.freq();
                    }
                    // skip lists
                    for (long idx = 1; idx <= 8; idx++) {
                        cancellationChecker.logEvent();
                        final int skipDocID = Math.toIntExact(idx * reader.maxDoc() / 8);
                        postings = termsEnum.postings(postings, PostingsEnum.NONE);
                        if (postings.advance(skipDocID) != DocIdSetIterator.NO_MORE_DOCS) {
                            postings.freq();
                            postings.nextDoc();
                        } else {
                            break;
                        }
                    }
                }
                postingsBytes = directory.getBytesRead() - termBytes;
                if (postingsBytes > 0) {
                    stats.addPosting(field.name, postingsBytes);
                }
            }

            // Visit positions, offsets, and payloads
            {
                TermsEnum termsEnum = terms.iterator();
                while (termsEnum.next() != null) {
                    cancellationChecker.logEvent();
                    termsEnum.docFreq();
                    termsEnum.totalTermFreq();
                    postings = termsEnum.postings(postings, PostingsEnum.ALL);
                    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        if (terms.hasPositions()) {
                            for (int pos = 0; pos < postings.freq(); pos++) {
                                postings.nextPosition();
                                postings.startOffset();
                                postings.endOffset();
                                postings.getPayload();
                            }
                        }
                    }
                }
                final long proximityBytes = directory.getBytesRead() - postingsBytes - termBytes;
                if (proximityBytes > 0) {
                    stats.addProximity(field.name, proximityBytes);
                }
            }
        }
    }

    void analyzePoints(SegmentReader reader, IndexDiskUsageStats stats) throws IOException {
        PointsReader pointsReader = reader.getPointsReader();
        if (pointsReader == null) {
            return;
        }
        pointsReader = pointsReader.getMergeInstance();
        final PointValues.IntersectVisitor visitor = new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                assert false : "Must never be called";
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                cancellationChecker.logEvent();
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
        for (FieldInfo field : reader.getFieldInfos()) {
            cancellationChecker.checkForCancellation();
            directory.resetAndTrackBytesRead(BytesReadTrackingMode.SIZE_ONLY);
            if (field.getPointDimensionCount() > 0) {
                final PointValues values = pointsReader.getValues(field.name);
                values.intersect(visitor);
                final long length = directory.getBytesRead();
                if (length > 0) {
                    stats.addPoints(field.name, length);
                }
            }
        }
    }

    void analyzeNorms(SegmentReader reader, IndexDiskUsageStats stats) throws IOException {
        NormsProducer normsReader = reader.getNormsReader();
        if (normsReader == null) {
            return;
        }
        normsReader = normsReader.getMergeInstance();
        for (FieldInfo field : reader.getFieldInfos()) {
            if (field.hasNorms()) {
                cancellationChecker.checkForCancellation();
                directory.resetAndTrackBytesRead(BytesReadTrackingMode.SIZE_ONLY);
                final NumericDocValues norms = normsReader.getNorms(field);
                while (norms.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    cancellationChecker.logEvent();
                    norms.longValue();
                }
                final long fieldLength = directory.getBytesRead();
                if (fieldLength > 0) {
                    stats.addNorms(field.name, fieldLength);
                }
            }
        }
    }

    void analyzeTermVectors(SegmentReader reader, IndexDiskUsageStats stats) throws IOException {
        TermVectorsReader termVectorsReader = reader.getTermVectorsReader();
        if (termVectorsReader == null) {
            return;
        }
        termVectorsReader = termVectorsReader.getMergeInstance();
        directory.resetAndTrackBytesRead(BytesReadTrackingMode.SIZE_AND_POSITION);
        final TermVectorsVisitor visitor = new TermVectorsVisitor();
        for (int docID = 0; docID < reader.numDocs(); docID++) {
            cancellationChecker.logEvent();
            final Fields vectors = termVectorsReader.get(docID);
            if (vectors != null) {
                for (String field : vectors) {
                    cancellationChecker.logEvent();
                    visitor.visitField(vectors, field);
                }
            }
        }
        if (visitor.fields.isEmpty() == false) {
            final long totalBytes = visitor.fields.values().stream().mapToLong(v -> v).sum();
            final double ratio = (double) (directory.getBytesRead()) / (double) (totalBytes);
            for (Map.Entry<String, Long> field : visitor.fields.entrySet()) {
                final long fieldBytes = (long) Math.ceil(field.getValue() * ratio);
                stats.addTermVectors(field.getKey(), fieldBytes);
            }
        }
    }

    private class TermVectorsVisitor {
        final Map<String, Long> fields = new HashMap<>();
        private PostingsEnum docsAndPositions; // to reuse

        void visitField(Fields vectors, String fieldName) throws IOException {
            final Terms terms = vectors.terms(fieldName);
            if (terms == null) {
                return;
            }
            final boolean hasPositions = terms.hasPositions();
            final boolean hasOffsets = terms.hasOffsets();
            final boolean hasPayloads = terms.hasPayloads();
            assert hasPayloads == false || hasPositions;
            long fieldLength = 1; // flags
            final TermsEnum termsEnum = terms.iterator();
            BytesRef bytesRef;
            while ((bytesRef = termsEnum.next()) != null) {
                cancellationChecker.logEvent();
                fieldLength += Integer.BYTES + bytesRef.length; // term
                final int freq = (int) termsEnum.totalTermFreq();
                fieldLength += Integer.BYTES; // freq
                if (hasPositions || hasOffsets) {
                    docsAndPositions = termsEnum.postings(docsAndPositions, PostingsEnum.OFFSETS | PostingsEnum.PAYLOADS);
                    assert docsAndPositions != null;
                    while (docsAndPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        cancellationChecker.logEvent();
                        assert docsAndPositions.freq() == freq;
                        for (int posUpTo = 0; posUpTo < freq; posUpTo++) {
                            final int pos = docsAndPositions.nextPosition();
                            fieldLength += Integer.BYTES; // position
                            docsAndPositions.startOffset();
                            fieldLength += Integer.BYTES; // start offset
                            docsAndPositions.endOffset();
                            fieldLength += Integer.BYTES; // end offset
                            final BytesRef payload = docsAndPositions.getPayload();
                            if (payload != null) {
                                fieldLength += Integer.BYTES + payload.length; // payload
                            }
                            assert hasPositions == false || pos >= 0;
                        }
                    }
                }
            }
            final long finalLength = fieldLength;
            fields.compute(fieldName, (k, v) -> v == null ? finalLength : v + finalLength);
        }
    }

    @Override
    public void close() throws IOException {
        directoryReader.close();
    }

    private static class TrackingReadBytesDirectory extends FilterDirectory {
        private final Map<String, BytesReadTracker> trackers = new HashMap<>();

        TrackingReadBytesDirectory(Directory in) {
            super(in);
        }

        void resetAndTrackBytesRead(BytesReadTrackingMode mode) {
            for (BytesReadTracker tracker : trackers.values()) {
                tracker.reset(mode);
            }
        }

        long getBytesRead() {
            long total = 0;
            for (BytesReadTracker tracker : trackers.values()) {
                total += tracker.getBytesRead();
            }
            return total;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            IndexInput in = super.openInput(name, context);
            try {
                final BytesReadTracker tracker = trackers.computeIfAbsent(name, k -> new BytesReadTracker());
                final TrackingReadBytesIndexInput wrapped = new TrackingReadBytesIndexInput(in, 0L, tracker);
                in = null;
                return wrapped;
            } finally {
                IOUtils.close(in);
            }
        }
    }

    private static class TrackingReadBytesIndexInput extends IndexInput {
        final IndexInput in;
        final BytesReadTracker bytesReadTracker;
        final long fileOffset;

        TrackingReadBytesIndexInput(IndexInput in, long fileOffset, BytesReadTracker bytesReadTracker) {
            super(in.toString());
            this.in = in;
            this.fileOffset = fileOffset;
            this.bytesReadTracker = bytesReadTracker;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
           return new TrackingReadBytesIndexInput(in.slice(sliceDescription, offset, length), this.fileOffset + offset, bytesReadTracker);
        }

        @Override
        public IndexInput clone() {
            return new TrackingReadBytesIndexInput(in.clone(), fileOffset, bytesReadTracker);
        }

        @Override
        public byte readByte() throws IOException {
            bytesReadTracker.trackPositions(fileOffset + getFilePointer(), 1);
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            bytesReadTracker.trackPositions(fileOffset + getFilePointer(), len);
            in.readBytes(b, offset, len);
        }
    }

    private enum BytesReadTrackingMode {
        SIZE_ONLY,
        SIZE_AND_POSITION
    }

    /**
     * Tracks the number of bytes that are read in {@link TrackingReadBytesDirectory}. This tracker supports two modes:
     * - {@link BytesReadTrackingMode#SIZE_ONLY} which is fast but can be inaccurate if the same position is read multiple times
     * - {@link BytesReadTrackingMode#SIZE_AND_POSITION} which is accurate but slower and uses more memory as it tracks
     * the positions of each byte read. We need to use this mode if the reader decodes/decompresses the same block/chunk
     * multiple times or access the same region multiple times.
     */
    private static class BytesReadTracker {
        private static final short PARTITION = 8192;
        private BytesReadTrackingMode mode = BytesReadTrackingMode.SIZE_ONLY;
        private long bytesRead;
        private final Map<Long, CountedBitSet> buckets = new HashMap<>();

        // Keep the last bitset to avoid looking up it in the HashMap as we should access bytes in the same range in adjacent reads.
        private long lastPositionKey = -1;
        private CountedBitSet lastBitSet = null;

        void trackPositions(long position, int length) {
            if (mode == BytesReadTrackingMode.SIZE_ONLY) {
                assert buckets.isEmpty();
                bytesRead += length;
            } else {
                assert bytesRead == 0;
                final long endPosition = position + length;
                for (; position < endPosition; position++) {
                    long key = position / PARTITION;
                    if (key != lastPositionKey) {
                        lastPositionKey = key;
                        lastBitSet = buckets.computeIfAbsent(lastPositionKey, k -> new CountedBitSet(PARTITION));
                    }
                    lastBitSet.set(Math.toIntExact(position % PARTITION));
                }
            }
        }

        void reset(BytesReadTrackingMode mode) {
            this.mode = mode;
            this.buckets.clear();
            this.bytesRead = 0;
            this.lastPositionKey = -1;
            this.lastBitSet = null;
        }

        long getBytesRead() {
            if (mode == BytesReadTrackingMode.SIZE_AND_POSITION) {
                assert bytesRead == 0;
                return buckets.values().stream().mapToLong(CountedBitSet::cardinality).sum();
            } else {
                assert buckets.isEmpty();
                return bytesRead;
            }
        }
    }

    /**
     * Periodically checks if the task was cancelled so the analyzing process can abort quickly.
     */
    private static class CancellationChecker {
        static final long THRESHOLD = 10_000;
        private long iterations;
        private final Runnable checkForCancellationRunner;

        CancellationChecker(Runnable checkForCancellationRunner) {
            this.checkForCancellationRunner = checkForCancellationRunner;
        }

        void logEvent() {
            if (iterations == THRESHOLD) {
                checkForCancellation();
            } else {
                iterations++;
            }
        }

        void checkForCancellation() {
            iterations = 0;
            checkForCancellationRunner.run();
        }
    }
}
