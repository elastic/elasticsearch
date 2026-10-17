/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.column.Column;
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
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ColumnarSourceWriter {

    static final int DOC_ID = 0;
    static final int[] DOC_IDS = new int[] { DOC_ID };

    private final SourceFilter sourceFilter;
    private final ThreadLocal<PerThreadResources> cachedColumnarPerThread;

    ColumnarSourceWriter(SourceFilter sourceFilter) {
        this.sourceFilter = sourceFilter;
        this.cachedColumnarPerThread = new ThreadLocal<>();
    }

    /**
     * Reconstructs the {@code _source} of one document from the fields of {@code doc}. Shared by the row path, which parses {@code doc}
     * from the original source, and the columnar batch path, which reassembles it from the mapped columns of one row.
     *
     * @param allDocs the full in-memory document tree (root plus nested children), so that nested loaders can select their children.
     *                Shard-index order places each child before its parent, matching the order in which the synthetic source loader
     *                reads them from a real segment; this is what preserves array order across nested documents, including the deeper
     *                documents that subobjects:false creates for object sub-fields and arrays inside a nested field. Without nested
     *                fields it is just {@code [doc]} and nothing downstream looks at it.
     */
    void write(MappingLookup mappingLookup, List<LuceneDocument> allDocs, LuceneDocument doc, XContentBuilder builder) throws IOException {
        PerThreadResources perThread = perThreadResources(mappingLookup);

        perThread.leafReader().setAllDocs(allDocs);
        perThread.leafReader().repopulate(doc);
        perThread.fieldLoader().reset();
        final SourceLoader.Leaf leaf = perThread.sourceLoaderLeaf();
        final LeafReaderContext leafCtx = perThread.leafReader.getContext();

        // A new leaf loader per document: it remembers the last doc it read, and every document reuses DOC_ID.
        var storedFieldLoader = perThread.storedFieldLoader().getLoader(leafCtx, DOC_IDS);
        storedFieldLoader.advanceTo(DOC_ID);
        leaf.write(storedFieldLoader, DOC_ID, builder);
    }

    /**
     * Creates a writer that rebuilds the {@code _source} of every document of one batch, in document order, straight from the mapped
     * columns. It binds each column to the leaf-reader slot(s) its values fill once up front, so that per document the values flow
     * directly from the column cursors into the slots. This replaces, per document, the {@code IndexableField} assembly of
     * {@code MappedColumns.RowCursor} and the per-field map lookups and type dispatch of
     * {@link ReusableColumnarStoredLeafReader#repopulate} that reading those fields back would cost. Columns whose fields no loader
     * reads (no slot was registered for their name, nothing stored) are skipped entirely.
     *
     * <p>The returned writer is bound to the calling thread's loader resources: it must be used on the calling thread and not
     * interleaved with {@link #write}.
     *
     * @param columns the columns to rebuild documents from — exactly those whose fields the row path would feed to
     *                {@link ReusableColumnarStoredLeafReader#repopulate}
     */
    BatchWriter batchWriter(MappingLookup mappingLookup, List<LuceneColumn> columns) throws IOException {
        return new BatchWriter(perThreadResources(mappingLookup), columns);
    }

    /**
     * It is safe to reuse the synthetic loader and leaf loader for each thread per index, because a new mapping results in a new
     * instance of this class and otherwise materialized mappings stay immutable.
     */
    private PerThreadResources perThreadResources(MappingLookup mappingLookup) throws IOException {
        PerThreadResources perThread = cachedColumnarPerThread.get();
        if (perThread == null) {
            final Mapping mapping = mappingLookup.getMapping();
            final SourceFilter blobFilter = blobSourceFilter(mappingLookup);
            SourceLoader.SyntheticFieldLoader fieldLoader = mapping.syntheticFieldLoader(blobFilter);
            final SourceLoader.Synthetic sourceLoader = new SourceLoader.Synthetic(blobFilter, () -> {
                fieldLoader.reset();
                return fieldLoader;
            }, SourceFieldMetrics.NOOP, mapping.ignoredSourceFormat());

            ReusableColumnarStoredLeafReader leafReader = new ReusableColumnarStoredLeafReader();
            SourceLoader.Leaf leaf = sourceLoader.leaf(leafReader.getContext(), DOC_IDS);
            // The required stored fields only depend on the mapping, so the factory is built once. Columnar_stored documents
            // normally need none, and then nothing has to be visited at all.
            final Set<String> requiredStoredFields = sourceLoader.requiredStoredFields();
            final StoredFieldLoader storedFieldLoader = requiredStoredFields.isEmpty()
                ? StoredFieldLoader.empty()
                : StoredFieldLoader.create(false, requiredStoredFields);
            perThread = new PerThreadResources(sourceLoader, fieldLoader, leaf, leafReader, storedFieldLoader);
            cachedColumnarPerThread.set(perThread);
        }
        return perThread;
    }

    /**
     * Rebuilds the {@code _source} of one batch's documents from its columns, one document per {@link #writeNext} call.
     *
     * @see #batchWriter(MappingLookup, List)
     */
    static final class BatchWriter {

        private final PerThreadResources perThread;
        private final LuceneColumn.RowValueCursor[] valueCursors;
        private final LuceneColumn.RowValueSink[] valueSinks;
        private final int[] valueHeads;
        private final LuceneColumn.RowFieldCursor[] storedFieldCursors;
        private final int[] storedFieldHeads;
        private int doc;

        private BatchWriter(PerThreadResources perThread, List<LuceneColumn> columns) {
            this.perThread = perThread;
            final ReusableColumnarStoredLeafReader reader = perThread.leafReader();
            // No per-document LuceneDocument tree exists on this path; the batch path does not support nested fields, which are the
            // only consumers of allDocs and the current document.
            reader.setAllDocs(List.of());
            final List<LuceneColumn.RowValueCursor> cursors = new ArrayList<>(columns.size());
            final List<LuceneColumn.RowValueSink> sinks = new ArrayList<>(columns.size());
            final List<LuceneColumn.RowFieldCursor> storedCursors = new ArrayList<>();
            for (LuceneColumn column : columns) {
                final Column luceneColumn = column.toLuceneColumn();
                final LuceneColumn.RowValueSink sink = reader.slotSink(luceneColumn.name(), luceneColumn.fieldType().docValuesType());
                if (sink != null) {
                    cursors.add(column.rowValueCursor());
                    sinks.add(sink);
                }
                // Mirrors repopulate: a stored field joins the reader's stored-fields list regardless of its doc values. Stored
                // fields keep the field-cursor form, as that list is read through a StoredFieldVisitor over IndexableFields.
                if (luceneColumn.fieldType().stored()) {
                    storedCursors.add(column.rowFieldCursor());
                }
            }
            this.valueCursors = cursors.toArray(new LuceneColumn.RowValueCursor[0]);
            this.valueSinks = sinks.toArray(new LuceneColumn.RowValueSink[0]);
            this.valueHeads = new int[valueCursors.length];
            for (int c = 0; c < valueCursors.length; c++) {
                valueHeads[c] = valueCursors[c].nextDoc();
            }
            this.storedFieldCursors = storedCursors.toArray(new LuceneColumn.RowFieldCursor[0]);
            this.storedFieldHeads = new int[storedFieldCursors.length];
            for (int c = 0; c < storedFieldCursors.length; c++) {
                storedFieldHeads[c] = storedFieldCursors[c].nextDoc();
            }
        }

        /**
         * Rebuilds the next document's {@code _source} into {@code builder}. Must be called exactly once per document of the batch,
         * in document order, including for documents that will not be written.
         */
        void writeNext(XContentBuilder builder) throws IOException {
            final ReusableColumnarStoredLeafReader reader = perThread.leafReader();
            reader.beginDoc();
            for (int c = 0; c < valueCursors.length; c++) {
                while (valueHeads[c] == doc) {
                    valueCursors[c].appendCurrentValue(valueSinks[c]);
                    valueHeads[c] = valueCursors[c].nextDoc();
                }
            }
            for (int c = 0; c < storedFieldCursors.length; c++) {
                while (storedFieldHeads[c] == doc) {
                    storedFieldCursors[c].appendCurrentFields(reader.storedFields);
                    storedFieldHeads[c] = storedFieldCursors[c].nextDoc();
                }
            }
            doc++;

            perThread.fieldLoader().reset();
            // A new leaf loader per document: it remembers the last doc it read, and every document reuses DOC_ID.
            var storedFieldLoader = perThread.storedFieldLoader().getLoader(reader.getContext(), DOC_IDS);
            storedFieldLoader.advanceTo(DOC_ID);
            perThread.sourceLoaderLeaf().write(storedFieldLoader, DOC_ID, builder);
        }
    }

    /**
     * The filter to build the blob with: the mapping's own {@code _source} filter, plus every field patched back into
     * {@code _source} on read ({@link MappingLookup#syntheticVectorFields()}). Those are held in the index already, so
     * writing them into the blob would store them twice.
     */
    private SourceFilter blobSourceFilter(MappingLookup mappingLookup) {
        Set<String> vectorFields = mappingLookup.syntheticVectorFields();
        if (vectorFields.isEmpty()) {
            return sourceFilter;
        }
        if (sourceFilter == null) {
            return new SourceFilter(null, vectorFields.toArray(String[]::new));
        }
        // the mapping's own filter may already name a vector field, and duplicate excludes are rejected downstream
        Set<String> excludes = new LinkedHashSet<>(Arrays.asList(sourceFilter.getExcludes()));
        excludes.addAll(vectorFields);
        return new SourceFilter(sourceFilter.getIncludes(), excludes.toArray(String[]::new));
    }

    private record PerThreadResources(
        SourceLoader.Synthetic sourceLoader,
        SourceLoader.SyntheticFieldLoader fieldLoader,
        SourceLoader.Leaf sourceLoaderLeaf,
        ReusableColumnarStoredLeafReader leafReader,
        StoredFieldLoader storedFieldLoader
    ) {}

    static class ReusableColumnarStoredLeafReader extends LeafReader {

        private static final String COUNT_FIELD_SUFFIX = MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
        private static final FieldInfos EMPTY_FIELD_INFOS = new FieldInfos(new FieldInfo[0]) {

            @Override
            public FieldInfo fieldInfo(String fieldName) {
                // Shortcut: no need to check an empty map.
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

        /** The slots the current document filled, so that {@link #repopulate} only has to clear those for the next one. */
        private final List<Slot> dirtySlots = new ArrayList<>();

        // -------------------------------------------------------------------------
        // Nested reconstruction state
        // -------------------------------------------------------------------------

        // All in-memory documents for the root being reconstructed (root + nested children), in document order.
        // Shared by reference across the reader instances of every nesting level so a nested loader can select its
        // children by parent-pointer match. Empty when the index has no nested fields.
        private List<LuceneDocument> allDocs = List.of();
        // The document this reader currently holds (set by repopulate). A nested loader reads this to know whose
        // children to reconstruct.
        private LuceneDocument currentDoc;

        void setAllDocs(List<LuceneDocument> allDocs) {
            this.allDocs = allDocs;
        }

        List<LuceneDocument> allDocs() {
            return allDocs;
        }

        LuceneDocument currentDoc() {
            return currentDoc;
        }

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
            beginDoc();
            this.currentDoc = doc;

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
                                dirtySlots.add(slot);
                            }
                        } else {
                            NumericSlot slot = numericSlots.get(name);
                            if (slot != null && slot.present == false) {
                                slot.value = field.numericValue().longValue();
                                slot.present = true;
                                dirtySlots.add(slot);
                            }
                        }
                    }
                    case BINARY -> {
                        BinarySlot slot = binarySlots.get(field.name());
                        if (slot != null && slot.present == false) {
                            slot.value = field.binaryValue();
                            slot.present = true;
                            dirtySlots.add(slot);
                        }
                    }
                    case SORTED -> {
                        // A SORTED field may be registered as a SortedSetSlot (when first accessed
                        // via DocValues.getSortedSet → getSortedSetDocValues) or as a SortedSlot
                        // (when accessed directly via getSortedDocValues).
                        SortedSetSlot ss = sortedSetSlots.get(field.name());
                        if (ss != null) {
                            if (ss.count == 0) {
                                dirtySlots.add(ss);
                            }
                            ss.add(field.binaryValue());
                        }
                        SortedSlot s = sortedSlots.get(field.name());
                        if (s != null && s.present == false) {
                            s.value = field.binaryValue();
                            s.present = true;
                            dirtySlots.add(s);
                        }
                    }
                    case SORTED_NUMERIC -> {
                        SortedNumericSlot slot = sortedNumericSlots.get(field.name());
                        if (slot != null) {
                            if (slot.count == 0) {
                                dirtySlots.add(slot);
                            }
                            slot.add(field.numericValue().longValue());
                        }
                    }
                    case SORTED_SET -> {
                        SortedSetSlot slot = sortedSetSlots.get(field.name());
                        if (slot != null) {
                            if (slot.count == 0) {
                                dirtySlots.add(slot);
                            }
                            slot.add(field.binaryValue());
                        }
                    }
                    default -> {
                    } // NONE: no doc values to fill
                }

                // TODO: look into removing, with columnar source we shouldn't have any stored fields:
                // this is actually only needed for randomized block loader tests which tests columnar source with binary doc values
                // disabled:
                if (field.fieldType().stored()) {
                    storedFields.add(field);
                }
            }
        }

        /**
         * Clears everything the previous document left behind: the slots it filled, its stored fields, and the current document
         * reference. Only the previously filled slots can hold state, and there are typically far fewer of them than registered
         * slots, so this avoids visiting every slot of the mapping for every document. {@link #repopulate} calls this before
         * reading a parsed document's fields; the batch path ({@code BatchWriter}) calls it directly before pushing one document's
         * column values into the slots.
         */
        void beginDoc() {
            this.currentDoc = null;
            for (int i = 0; i < dirtySlots.size(); i++) {
                dirtySlots.get(i).clear();
            }
            dirtySlots.clear();
            storedFields.clear();
        }

        /**
         * Resolves the sink that feeds {@code fieldName}'s values into the slot(s) registered for that name, or {@code null} when
         * no loader registered one (the batch path then skips the column entirely, as {@link #repopulate} would ignore its fields).
         * One resolution per column per batch replaces the per-field-per-document map lookups and type dispatch of
         * {@link #repopulate}; each sink mirrors that method's dirty tracking and its first-value-wins semantics for single-valued
         * slots. Must only be called after the one-time {@link SourceLoader.Synthetic#leaf} build, like {@link #repopulate}.
         */
        LuceneColumn.RowValueSink slotSink(String fieldName, DocValuesType dvType) {
            return switch (dvType) {
                case NUMERIC -> {
                    if (fieldName.endsWith(COUNT_FIELD_SUFFIX)) {
                        final CountsCompanionSlot slot = countsSlots.get(fieldName);
                        yield slot == null ? null : new LuceneColumn.RowValueSink() {
                            @Override
                            public void longValue(long value) {
                                if (slot.present == false) {
                                    slot.value = value;
                                    slot.present = true;
                                    dirtySlots.add(slot);
                                }
                            }
                        };
                    }
                    final NumericSlot slot = numericSlots.get(fieldName);
                    yield slot == null ? null : new LuceneColumn.RowValueSink() {
                        @Override
                        public void longValue(long value) {
                            if (slot.present == false) {
                                slot.value = value;
                                slot.present = true;
                                dirtySlots.add(slot);
                            }
                        }
                    };
                }
                case BINARY -> {
                    final BinarySlot slot = binarySlots.get(fieldName);
                    yield slot == null ? null : new LuceneColumn.RowValueSink() {
                        @Override
                        public void binaryValue(BytesRef value) {
                            if (slot.present == false) {
                                slot.value = value;
                                slot.present = true;
                                dirtySlots.add(slot);
                            }
                        }
                    };
                }
                case SORTED -> {
                    // A SORTED field may be registered as a SortedSetSlot (when first accessed via DocValues.getSortedSet →
                    // getSortedSetDocValues) or as a SortedSlot (when accessed directly via getSortedDocValues), or both.
                    final SortedSetSlot sortedSet = sortedSetSlots.get(fieldName);
                    final SortedSlot sorted = sortedSlots.get(fieldName);
                    if (sortedSet == null && sorted == null) {
                        yield null;
                    }
                    yield new LuceneColumn.RowValueSink() {
                        @Override
                        public void binaryValue(BytesRef value) {
                            if (sortedSet != null) {
                                if (sortedSet.count == 0) {
                                    dirtySlots.add(sortedSet);
                                }
                                sortedSet.add(value);
                            }
                            if (sorted != null && sorted.present == false) {
                                sorted.value = value;
                                sorted.present = true;
                                dirtySlots.add(sorted);
                            }
                        }
                    };
                }
                case SORTED_NUMERIC -> {
                    final SortedNumericSlot slot = sortedNumericSlots.get(fieldName);
                    yield slot == null ? null : new LuceneColumn.RowValueSink() {
                        @Override
                        public void longValue(long value) {
                            if (slot.count == 0) {
                                dirtySlots.add(slot);
                            }
                            slot.add(value);
                        }
                    };
                }
                case SORTED_SET -> {
                    final SortedSetSlot slot = sortedSetSlots.get(fieldName);
                    yield slot == null ? null : new LuceneColumn.RowValueSink() {
                        @Override
                        public void binaryValue(BytesRef value) {
                            if (slot.count == 0) {
                                dirtySlots.add(slot);
                            }
                            slot.add(value);
                        }
                    };
                }
                case NONE -> null;
            };
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

        // TODO: look into removing this, columnar source shouldn't use any stored fields.
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

        private static FieldInfo fieldInfo(String name) {
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
        public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
            return null;
        }

        @Override
        public FieldInfos getFieldInfos() {
            return EMPTY_FIELD_INFOS;
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

        /** A slot that holds the value(s) of one field for the current document and can be emptied for the next one. */
        private interface Slot {
            void clear();
        }

        /**
         * A stable, reusable numeric doc-values iterator backed by a single mutable slot.
         */
        private static final class NumericSlot extends NumericDocValues implements Slot {
            boolean present;
            long value;

            @Override
            public void clear() {
                present = false;
            }

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
                // Like every slot below: after a successful advanceExact the iterator is positioned on the target, as it is for a real
                // segment, and loaders may assert on it, for example the pattern_text one before it reads a value.
                return present ? DOC_ID : DocIdSetIterator.NO_MORE_DOCS;
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
         * {@link org.elasticsearch.index.fielddata.MultiValuedSortableBinaryDocValues}.
         *
         * <p>When the counts field is absent from the document but the binary payload is present,
         * {@link #advanceExact} returns {@code true} and {@link #longValue} returns {@code 1},
         * causing {@link org.elasticsearch.index.fielddata.MultiValuedSortableBinaryDocValues.SeparateCounts}
         * to behave identically to {@code PlainBinary} for single-valued fields.
         */
        private static final class CountsCompanionSlot extends NumericDocValues implements Slot {
            boolean present;
            long value;
            final BinarySlot binarySlot;

            @Override
            public void clear() {
                present = false;
            }

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
                return present || binarySlot.present ? DOC_ID : DocIdSetIterator.NO_MORE_DOCS;
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
        private static final class BinarySlot extends BinaryDocValues implements Slot {
            boolean present;
            BytesRef value;

            @Override
            public void clear() {
                present = false;
            }

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
                return present ? DOC_ID : DocIdSetIterator.NO_MORE_DOCS;
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
        private static final class SortedSlot extends SortedDocValues implements Slot {
            boolean present;
            BytesRef value;

            @Override
            public void clear() {
                present = false;
            }

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
                return present ? DOC_ID : DocIdSetIterator.NO_MORE_DOCS;
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
         * Values are inserted in sorted ascending order by {@link #add}, maintaining Lucene's invariant
         * without a separate post-population sort pass.
         */
        private static final class SortedNumericSlot extends SortedNumericDocValues implements Slot {
            long[] values = new long[4];
            int count;
            private int cursor;

            @Override
            public void clear() {
                count = 0;
            }

            void add(long v) {
                if (count == values.length) {
                    values = Arrays.copyOf(values, values.length * 2);
                }
                // Insertion sort: find position and shift right to maintain ascending order.
                int pos = Arrays.binarySearch(values, 0, count, v);
                if (pos < 0) pos = -pos - 1;
                System.arraycopy(values, pos, values, pos + 1, count - pos);
                values[pos] = v;
                count++;
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
                return count > 0 ? DOC_ID : DocIdSetIterator.NO_MORE_DOCS;
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
         * Values are inserted in sorted, deduplicated order by {@link #add}, maintaining Lucene's
         * invariant without a separate post-population sort/dedup pass.
         * {@link #getValueCount()} always returns at least {@code 1} so that build-time callers
         * (e.g. {@link SortedSetDocValuesSyntheticFieldLoaderLayer#docValuesLoader}) do not
         * permanently bind to {@code NO_VALUES}.
         */
        private static final class SortedSetSlot extends SortedSetDocValues implements Slot {
            BytesRef[] values = new BytesRef[4];
            int count;
            private int cursor;

            @Override
            public void clear() {
                count = 0;
            }

            void add(BytesRef v) {
                if (count == values.length) {
                    values = Arrays.copyOf(values, values.length * 2);
                }
                // Binary search for insertion point; skip if already present (dedup).
                int lo = 0, hi = count;
                while (lo < hi) {
                    int mid = (lo + hi) >>> 1;
                    int cmp = values[mid].compareTo(v);
                    if (cmp < 0) lo = mid + 1;
                    else if (cmp > 0) hi = mid;
                    else return;
                }
                System.arraycopy(values, lo, values, lo + 1, count - lo);
                values[lo] = v;
                count++;
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
                return count > 0 ? DOC_ID : DocIdSetIterator.NO_MORE_DOCS;
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
