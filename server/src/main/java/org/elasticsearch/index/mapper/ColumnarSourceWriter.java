/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class ColumnarSourceWriter {

    private static final int DOC_ID = 0;
    private static final int[] DOC_IDS = new int[] { DOC_ID };

    private final SourceFilter sourceFilter;
    private final ThreadLocal<ColumnarPerThread> cachedColumnarPerThread;

    public ColumnarSourceWriter(SourceFilter sourceFilter) {
        this.sourceFilter = sourceFilter;
        this.cachedColumnarPerThread = new ThreadLocal<>();
    }

    void write(DocumentParserContext context, XContentBuilder builder) throws IOException {
        // It is safe to reuse synthic loader and leaf loader for each thread per index.
        // Because a new mapping will result into a new instance of this class and otherwise materialized mappings stay immutable.
        ColumnarPerThread perThread = cachedColumnarPerThread.get();
        if (perThread == null) {
            final Mapping mapping = context.mappingLookup().getMapping();
            SourceLoader.SyntheticFieldLoader fieldLoader = mapping.syntheticFieldLoader(sourceFilter);
            final SourceLoader.Synthetic sourceLoader = new SourceLoader.Synthetic(sourceFilter, () -> {
                fieldLoader.reset();
                return fieldLoader;
            }, SourceFieldMetrics.NOOP, mapping.ignoredSourceFormat());

            ReusableColumnarStoredLeafReader reader = new ReusableColumnarStoredLeafReader();
            LeafReaderContext leafCtx = reader.getContext();
            SourceLoader.Leaf leaf = sourceLoader.leaf(reader, DOC_IDS);
            perThread = new ColumnarPerThread(sourceLoader, reader, leafCtx, fieldLoader, leaf);
            cachedColumnarPerThread.set(perThread);
        }

        perThread.reader().repopulate(context.doc());
        perThread.loader().reset();
        final SourceLoader.Synthetic sourceLoader = perThread.sourceLoader;
        final SourceLoader.Leaf leaf = perThread.leaf();
        final LeafReaderContext leafCtx = perThread.leafCtx();

        // TODO: in columnar there shouldn't exist any store fields and so we can use StoredFieldLoader.empty() here.
        var storedFieldLoader = StoredFieldLoader.create(false, sourceLoader.requiredStoredFields()).getLoader(leafCtx, DOC_IDS);
        storedFieldLoader.advanceTo(DOC_ID);
        leaf.write(storedFieldLoader, DOC_ID, builder);
    }

    private record ColumnarPerThread(
        SourceLoader.Synthetic sourceLoader,
        ReusableColumnarStoredLeafReader reader,
        LeafReaderContext leafCtx,
        SourceLoader.SyntheticFieldLoader loader,
        SourceLoader.Leaf leaf
    ) {}

    /**
     * A reusable {@link LeafReader} for the {@code columnar_stored} synthetic-source path.
     *
     * <p>This reader maintains per-field <em>slots</em> whose contents are swapped via
     * {@link #repopulate(LuceneDocument)} on each document. The slot map itself is built lazily on the
     * first call to any {@code getXxxDocValues} accessor (during the one-time
     * {@link SourceLoader.Synthetic#leaf} build) and then frozen — only slot contents change.
     *
     * <p><strong>Correctness invariants:</strong>
     * <ol>
     *   <li>Every schema field must yield a non-null (and for sorted-set, {@code getValueCount() ≥ 1})
     *       stable iterator the first time the loader asks for it, so no DV layer permanently binds
     *       to {@code NO_VALUES}.</li>
     *   <li>{@code repopulate} fills only registered slots; unregistered fields in the document are
     *       ignored. Registered slots that are absent in the current document remain cleared (absent)
     *       for that document.</li>
     *   <li>For {@code .counts} companion fields (used by
     *       {@link org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues}), a
     *       {@link CountsCompanionSlot} is registered that provides a virtual count of {@code 1} when
     *       the binary payload is present but the counts field is absent. This ensures
     *       {@code SeparateCounts} format is chosen at build time and behaves like {@code PlainBinary}
     *       for single-valued binary fields.</li>
     * </ol>
     *
     */
    static class ReusableColumnarStoredLeafReader extends LeafReader {

        private static final String COUNT_FIELD_SUFFIX = MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

        private static final FieldInfos EMPTY_FIELD_INFOS = new FieldInfos(new FieldInfo[0]) {

            @Override
            public FieldInfo fieldInfo(String fieldName) {
                // It is ok to return null here, since this will be used by DocValues#checkField(...) to return an empty doc values
                // instance.
                return null;
            }

        };

        // -------------------------------------------------------------------------
        // Slot maps — lazily populated during the one-time leaf() build
        // -------------------------------------------------------------------------

        /** Plain numeric slots (excludes {@code .counts} companions). */
        private final Map<String, NumericSlot> numericSlots = new HashMap<>();
        /** Counts companion slots, keyed by the full {@code "field.counts"} name. */
        private final Map<String, CountsCompanionSlot> countsSlots = new HashMap<>();
        private final Map<String, BinarySlot> binarySlots = new HashMap<>();
        /** SORTED-type slots created via {@link #getSortedDocValues}. */
        private final Map<String, SortedSlot> sortedSlots = new HashMap<>();
        /**
         * SORTED_SET-type slots created via {@link #getSortedSetDocValues}.
         * SORTED-type fields in the document are also reflected here when they were first accessed
         * through {@link #getSortedSetDocValues} (e.g. via {@link org.apache.lucene.index.DocValues#getSortedSet}).
         */
        private final Map<String, SortedSetSlot> sortedSetSlots = new HashMap<>();
        private final Map<String, SortedNumericSlot> sortedNumericSlots = new HashMap<>();

        // -------------------------------------------------------------------------
        // Stored fields for the current document
        // -------------------------------------------------------------------------

        private final List<IndexableField> storedFields = new ArrayList<>();

        // -------------------------------------------------------------------------
        // Dirty tracking — reset before each repopulate
        // -------------------------------------------------------------------------

        private final List<NumericSlot> dirtyNumericSlots = new ArrayList<>();
        private final List<CountsCompanionSlot> dirtyCountsSlots = new ArrayList<>();
        private final List<BinarySlot> dirtyBinarySlots = new ArrayList<>();
        private final List<SortedSlot> dirtySortedSlots = new ArrayList<>();
        private final List<SortedSetSlot> dirtySortedSetSlots = new ArrayList<>();
        private final List<SortedNumericSlot> dirtySortedNumericSlots = new ArrayList<>();

        // -------------------------------------------------------------------------
        // Document repopulation
        // -------------------------------------------------------------------------

        /**
         * Swaps in the contents of {@code doc}: clears all slots populated during the previous call,
         * then fills slots for each field present in {@code doc}. Slots registered in the slot map but
         * absent from {@code doc} remain cleared (i.e., {@code advanceExact(0)} returns {@code false}
         * for the duration of this document).
         *
         * <p>This method must be called <em>after</em> the one-time {@link SourceLoader.Synthetic#leaf}
         * build so that the slot map is fully initialized.
         */
        void repopulate(LuceneDocument doc) {
            // Clear dirty slots from the previous document.
            for (NumericSlot slot : dirtyNumericSlots) {
                slot.present = false;
            }
            dirtyNumericSlots.clear();
            for (CountsCompanionSlot slot : dirtyCountsSlots) {
                slot.present = false;
            }
            dirtyCountsSlots.clear();
            for (BinarySlot slot : dirtyBinarySlots) {
                slot.present = false;
            }
            dirtyBinarySlots.clear();
            for (SortedSlot slot : dirtySortedSlots) {
                slot.present = false;
            }
            dirtySortedSlots.clear();
            for (SortedSetSlot slot : dirtySortedSetSlots) {
                slot.count = 0;
            }
            dirtySortedSetSlots.clear();
            for (SortedNumericSlot slot : dirtySortedNumericSlots) {
                slot.count = 0;
            }
            dirtySortedNumericSlots.clear();
            storedFields.clear();

            // Fill slots from this document's fields.
            for (IndexableField field : doc.getFields()) {
                DocValuesType dvType = field.fieldType().docValuesType();
                switch (dvType) {
                    case NUMERIC -> {
                        String name = field.name();
                        if (name.endsWith(COUNT_FIELD_SUFFIX)) {
                            CountsCompanionSlot slot = countsSlots.get(name);
                            if (slot != null && slot.present == false) {
                                slot.value = field.numericValue().longValue();
                                slot.present = true;
                                dirtyCountsSlots.add(slot);
                            }
                        } else {
                            NumericSlot slot = numericSlots.get(name);
                            if (slot != null && slot.present == false) {
                                slot.value = field.numericValue().longValue();
                                slot.present = true;
                                dirtyNumericSlots.add(slot);
                            }
                        }
                    }
                    case BINARY -> {
                        BinarySlot slot = binarySlots.get(field.name());
                        if (slot != null && slot.present == false) {
                            slot.value = field.binaryValue();
                            slot.present = true;
                            dirtyBinarySlots.add(slot);
                        }
                    }
                    case SORTED -> {
                        // A SORTED field may be registered as a SortedSetSlot (when first accessed
                        // via DocValues.getSortedSet → getSortedSetDocValues) or as a SortedSlot
                        // (when accessed directly via getSortedDocValues).
                        SortedSetSlot ss = sortedSetSlots.get(field.name());
                        if (ss != null) {
                            ss.add(field.binaryValue());
                            if (ss.count == 1) {
                                dirtySortedSetSlots.add(ss);
                            }
                        }
                        SortedSlot s = sortedSlots.get(field.name());
                        if (s != null && s.present == false) {
                            s.value = field.binaryValue();
                            s.present = true;
                            dirtySortedSlots.add(s);
                        }
                    }
                    case SORTED_NUMERIC -> {
                        SortedNumericSlot slot = sortedNumericSlots.get(field.name());
                        if (slot != null) {
                            slot.add(field.numericValue().longValue());
                            if (slot.count == 1) {
                                dirtySortedNumericSlots.add(slot);
                            }
                        }
                    }
                    case SORTED_SET -> {
                        SortedSetSlot slot = sortedSetSlots.get(field.name());
                        if (slot != null) {
                            slot.add(field.binaryValue());
                            if (slot.count == 1) {
                                dirtySortedSetSlots.add(slot);
                            }
                        }
                    }
                    default -> {
                    } // NONE: no doc values to fill
                }
                // Mirror ColumnarStoredLeafReader's storedFields bucketing: stored fields for SORTED_NUMERIC,
                // SORTED_SET, and NONE-typed fields are surfaced via storedFields(); BINARY/NUMERIC/SORTED-typed
                // fields are not.
                if (dvType != DocValuesType.BINARY && dvType != DocValuesType.NUMERIC && dvType != DocValuesType.SORTED) {
                    if (field.fieldType().stored()) {
                        storedFields.add(field);
                    }
                }
            }

            // Sort SORTED_NUMERIC values ascending (Lucene's invariant).
            for (SortedNumericSlot slot : dirtySortedNumericSlots) {
                Arrays.sort(slot.values, 0, slot.count);
            }

            // Sort and deduplicate SORTED_SET values (Lucene's invariant).
            for (SortedSetSlot slot : dirtySortedSetSlots) {
                Arrays.sort(slot.values, 0, slot.count);
                int distinctCount = 0;
                BytesRef prev = null;
                for (int i = 0; i < slot.count; i++) {
                    if (prev == null || prev.compareTo(slot.values[i]) != 0) {
                        slot.values[distinctCount++] = slot.values[i];
                        prev = slot.values[i];
                    }
                }
                slot.count = distinctCount;
            }
        }

        // -------------------------------------------------------------------------
        // Doc-values accessors — lazily register slots on first call (build phase)
        // -------------------------------------------------------------------------

        @Override
        public NumericDocValues getNumericDocValues(String fieldName) throws IOException {
            if (fieldName.endsWith(COUNT_FIELD_SUFFIX)) {
                // Companion counts field: create a CountsCompanionSlot that provides a virtual
                // count of 1 when binary is present but counts are absent (single-valued fields).
                String binaryFieldName = fieldName.substring(0, fieldName.length() - COUNT_FIELD_SUFFIX.length());
                BinarySlot binarySlot = binarySlots.computeIfAbsent(binaryFieldName, k -> new BinarySlot());
                return countsSlots.computeIfAbsent(fieldName, k -> new CountsCompanionSlot(binarySlot));
            }
            return numericSlots.computeIfAbsent(fieldName, k -> new NumericSlot());
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String fieldName) throws IOException {
            return binarySlots.computeIfAbsent(fieldName, k -> new BinarySlot());
        }

        @Override
        public SortedDocValues getSortedDocValues(String fieldName) throws IOException {
            return sortedSlots.computeIfAbsent(fieldName, k -> new SortedSlot());
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String fieldName) throws IOException {
            return sortedNumericSlots.computeIfAbsent(fieldName, k -> new SortedNumericSlot());
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String fieldName) throws IOException {
            return sortedSetSlots.computeIfAbsent(fieldName, k -> new SortedSetSlot());
        }

        // -------------------------------------------------------------------------
        // Stored fields
        // -------------------------------------------------------------------------

        @Override
        public StoredFields storedFields() throws IOException {
            return new StoredFields() {
                @Override
                public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                    for (IndexableField field : storedFields) {
                        FieldInfo fieldInfo = fieldInfo(field.name());
                        if (visitor.needsField(fieldInfo) != StoredFieldVisitor.Status.YES) {
                            continue;
                        }
                        if (field.numericValue() != null) {
                            Number v = field.numericValue();
                            if (v instanceof Integer) {
                                visitor.intField(fieldInfo, v.intValue());
                            } else if (v instanceof Long) {
                                visitor.longField(fieldInfo, v.longValue());
                            } else if (v instanceof Float) {
                                visitor.floatField(fieldInfo, v.floatValue());
                            } else if (v instanceof Double) {
                                visitor.doubleField(fieldInfo, v.doubleValue());
                            }
                        } else if (field.stringValue() != null) {
                            visitor.stringField(fieldInfo, field.stringValue());
                        } else if (field.binaryValue() != null) {
                            byte[] data = new byte[field.binaryValue().length];
                            System.arraycopy(field.binaryValue().bytes, field.binaryValue().offset, data, 0, data.length);
                            visitor.binaryField(fieldInfo, data);
                        }
                    }
                }
            };
        }

        static FieldInfo fieldInfo(String name) {
            return new FieldInfo(
                name,
                0,
                false,
                false,
                false,
                IndexOptions.NONE,
                DocValuesType.NONE,
                DocValuesSkipIndexType.NONE,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
        }

        // -------------------------------------------------------------------------
        // Minimal required overrides
        // -------------------------------------------------------------------------

        @Override
        public int maxDoc() {
            return 1;
        }

        @Override
        public FieldInfos getFieldInfos() {
            return EMPTY_FIELD_INFOS;
        }

        @Override
        public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
            return null;
        }

        // -------------------------------------------------------------------------
        // Unsupported operations
        // -------------------------------------------------------------------------

        @Override
        public CacheHelper getCoreCacheHelper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Terms terms(String field) throws IOException {
            return null;
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bits getLiveDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkIntegrity() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeafMetaData getMetaData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TermVectors termVectors() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doClose() throws IOException {
            throw new UnsupportedOperationException();
        }

        // -------------------------------------------------------------------------
        // Inner slot classes — one per doc-values type
        // -------------------------------------------------------------------------

        /**
         * A stable, reusable numeric doc-values iterator backed by a single mutable slot.
         */
        private static final class NumericSlot extends NumericDocValues {
            boolean present;
            long value;

            @Override
            public long longValue() {
                return value;
            }

            @Override
            public boolean advanceExact(int target) {
                assert target == 0;
                return present;
            }

            @Override
            public int docID() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 1;
            }
        }

        /**
         * A companion numeric slot for {@code "field.counts"} fields used by
         * {@link org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues}.
         *
         * <p>When the counts field is absent from the document but the binary payload is present,
         * {@link #advanceExact} returns {@code true} and {@link #longValue} returns {@code 1},
         * causing {@link org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues.SeparateCounts}
         * to behave identically to {@code PlainBinary} for single-valued fields.
         */
        private static final class CountsCompanionSlot extends NumericDocValues {
            boolean present;
            long value;
            final BinarySlot binarySlot;

            CountsCompanionSlot(BinarySlot binarySlot) {
                this.binarySlot = binarySlot;
            }

            @Override
            public long longValue() {
                return present ? value : 1L;
            }

            @Override
            public boolean advanceExact(int target) {
                assert target == 0;
                return present || binarySlot.present;
            }

            @Override
            public int docID() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 1;
            }
        }

        /**
         * A stable, reusable binary doc-values iterator backed by a single mutable slot.
         */
        private static final class BinarySlot extends BinaryDocValues {
            boolean present;
            BytesRef value;

            @Override
            public BytesRef binaryValue() {
                return value;
            }

            @Override
            public boolean advanceExact(int target) {
                assert target == 0;
                return present;
            }

            @Override
            public int docID() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 1;
            }
        }

        /**
         * A stable, reusable sorted doc-values iterator backed by a single mutable slot.
         * {@link #getValueCount()} always returns {@code 1} so that callers checking for an empty
         * vocabulary at build time (e.g. via {@link org.apache.lucene.index.DocValues#getSortedSet})
         * do not permanently bind to {@code NO_VALUES}.
         */
        private static final class SortedSlot extends SortedDocValues {
            boolean present;
            BytesRef value;

            @Override
            public int ordValue() {
                return 0;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return value;
            }

            @Override
            public int getValueCount() {
                // Always return 1 so that build-time callers do not bind permanently to NO_VALUES.
                return 1;
            }

            @Override
            public boolean advanceExact(int target) {
                assert target == 0;
                return present;
            }

            @Override
            public int docID() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 1;
            }
        }

        /**
         * A stable, reusable sorted-numeric doc-values iterator backed by a growable value array.
         * Values are sorted ascending in {@link #repopulate} before this iterator is used.
         */
        private static final class SortedNumericSlot extends SortedNumericDocValues {
            long[] values = new long[4];
            int count;
            private int cursor;

            void add(long v) {
                if (count == values.length) {
                    values = Arrays.copyOf(values, values.length * 2);
                }
                values[count++] = v;
            }

            @Override
            public long nextValue() {
                return values[cursor++];
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public boolean advanceExact(int target) {
                assert target == 0;
                cursor = 0;
                return count > 0;
            }

            @Override
            public int docID() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 1;
            }
        }

        /**
         * A stable, reusable sorted-set doc-values iterator backed by a growable value array.
         * Values are sorted and deduplicated in {@link #repopulate} before this iterator is used.
         * {@link #getValueCount()} always returns at least {@code 1} so that build-time callers
         * (e.g. {@link SortedSetDocValuesSyntheticFieldLoaderLayer#docValuesLoader}) do not
         * permanently bind to {@code NO_VALUES}.
         */
        private static final class SortedSetSlot extends SortedSetDocValues {
            BytesRef[] values = new BytesRef[4];
            int count;
            private int cursor;

            void add(BytesRef v) {
                if (count == values.length) {
                    values = Arrays.copyOf(values, values.length * 2);
                }
                values[count++] = v;
            }

            @Override
            public long nextOrd() {
                return cursor++;
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return values[(int) ord];
            }

            @Override
            public long getValueCount() {
                // Always return at least 1 so that build-time callers do not permanently bind to NO_VALUES.
                return Math.max(1, count);
            }

            @Override
            public boolean advanceExact(int target) {
                assert target == 0;
                cursor = 0;
                return count > 0;
            }

            @Override
            public int docID() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 1;
            }
        }

    }
}
