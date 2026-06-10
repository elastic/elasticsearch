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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link LeafReader} over a single {@link LuceneDocument} that serves doc values and stored fields
 * directly from the document's in-memory {@link IndexableField} list, without building a real Lucene index.
 *
 * <p>This reader is used by {@link SourceFieldMapper} in {@code columnar_stored} mode to drive
 * {@link SourceLoader.Synthetic} at index time, replacing the previous approach of writing each parsed
 * document into a throwaway {@code ByteBuffersDirectory}/{@code IndexWriter}/{@code DirectoryReader}
 * just to obtain a {@code LeafReader}.
 *
 * <p>Unlike {@link DocumentLeafReader} (which is used for index-time scripts and deliberately sorts but
 * does not deduplicate {@code SORTED_SET} values), this reader reproduces Lucene's codec semantics
 * exactly for {@link SortedSetDocValues}: values are <em>sorted and deduplicated</em>, with ordinals
 * assigned by sorted-distinct position. This is required because the offset arrays written by
 * {@link FieldArrayContext} at index time use the same sorted-distinct ordinal convention.
 *
 * <p>Fields are grouped by name once in the constructor (O(n) over the document's field list) so that
 * each doc-values accessor is O(values-for-field) rather than O(all-fields), avoiding O(fields²)
 * behaviour during synthetic-source reconstruction which touches every field.
 *
 * <p>Columnar mode disables nested objects, so there is always exactly one root document and
 * {@code maxDoc()} is {@code 1}. Only the methods called by the synthetic-source path are
 * implemented; all others throw {@link UnsupportedOperationException}.
 */
class ColumnarStoredLeafReader extends LeafReader {

    private final Map<String, List<IndexableField>> fieldsByName;

    /**
     * Constructs a reader over the given document.
     * The document's fields are grouped by name in a single pass so that subsequent accessor
     * calls are efficient.
     */
    ColumnarStoredLeafReader(LuceneDocument document) {
        Map<String, List<IndexableField>> map = new HashMap<>();
        for (IndexableField field : document.getFields()) {
            map.computeIfAbsent(field.name(), k -> new ArrayList<>()).add(field);
        }
        this.fieldsByName = map;
    }

    // -------------------------------------------------------------------------
    // Doc-values accessors
    // -------------------------------------------------------------------------

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        List<IndexableField> fields = fieldsByName.getOrDefault(field, List.of());
        List<Number> values = new ArrayList<>();
        for (IndexableField f : fields) {
            if (f.fieldType().docValuesType() == DocValuesType.NUMERIC) {
                values.add(f.numericValue());
            }
        }
        return numericDocValues(values);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        List<IndexableField> fields = fieldsByName.getOrDefault(field, List.of());
        List<BytesRef> values = new ArrayList<>();
        for (IndexableField f : fields) {
            if (f.fieldType().docValuesType() == DocValuesType.BINARY) {
                values.add(f.binaryValue());
            }
        }
        return binaryDocValues(values);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        List<IndexableField> fields = fieldsByName.getOrDefault(field, List.of());
        List<BytesRef> values = new ArrayList<>();
        for (IndexableField f : fields) {
            if (f.fieldType().docValuesType() == DocValuesType.SORTED) {
                values.add(f.binaryValue());
            }
        }
        return sortedDocValues(values);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        List<IndexableField> fields = fieldsByName.getOrDefault(field, List.of());
        List<Long> values = new ArrayList<>();
        for (IndexableField f : fields) {
            if (f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC) {
                values.add(f.numericValue().longValue());
            }
        }
        // Lucene SortedNumericDocValues: values sorted ascending, duplicates kept.
        Collections.sort(values);
        return sortedNumericDocValues(values);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        List<IndexableField> fields = fieldsByName.getOrDefault(field, List.of());
        List<BytesRef> raw = new ArrayList<>();
        for (IndexableField f : fields) {
            if (f.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
                raw.add(f.binaryValue());
            }
        }
        // Lucene SortedSetDocValues: values sorted AND deduplicated; ordinals are sorted-distinct positions.
        // This must match FieldArrayContext.encodeOffsetArray, which assigns ordinals via a TreeMap
        // (sorted-distinct iteration order). Reconstruction in ValuesWithOffsetsDocValuesLoader
        // indexes into ords[] of length docValueCount() (= distinct count), so failing to dedup
        // would corrupt multi-valued arrays that contain duplicate values.
        Collections.sort(raw);
        List<BytesRef> distinct = new ArrayList<>(raw.size());
        for (BytesRef v : raw) {
            if (distinct.isEmpty() || distinct.get(distinct.size() - 1).equals(v) == false) {
                distinct.add(v);
            }
        }
        return sortedSetDocValues(distinct);
    }

    // -------------------------------------------------------------------------
    // Stored fields
    // -------------------------------------------------------------------------

    @Override
    public StoredFields storedFields() throws IOException {
        return new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                for (List<IndexableField> fields : fieldsByName.values()) {
                    for (IndexableField field : fields) {
                        if (field.fieldType().stored() == false) {
                            continue;
                        }
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
                            // Copy to avoid exposing internal bytes array with offset/length considerations.
                            byte[] data = new byte[field.binaryValue().length];
                            System.arraycopy(field.binaryValue().bytes, field.binaryValue().offset, data, 0, data.length);
                            visitor.binaryField(fieldInfo, data);
                        }
                    }
                }
            }
        };
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
        return new FieldInfos(new FieldInfo[0]);
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        return null;
    }

    // -------------------------------------------------------------------------
    // Unsupported operations (not called by the synthetic-source reconstruction path)
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
        // No indexed terms in an in-memory document reader; returning null signals "field not present"
        // to callers such as LeafReader.postings(), which propagates null to DocCountFieldMapper.
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
    // Private helpers
    // -------------------------------------------------------------------------

    // Our StoredFieldsVisitor implementations only check the name of the passed-in FieldInfo,
    // so that's the only value we need to set here.
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

    private static NumericDocValues numericDocValues(List<Number> values) {
        if (values.isEmpty()) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new NumericDocValues() {
            @Override
            public long longValue() {
                return values.get(0).longValue();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    private static BinaryDocValues binaryDocValues(List<BytesRef> values) {
        if (values.isEmpty()) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new BinaryDocValues() {
            @Override
            public BytesRef binaryValue() {
                return values.get(0);
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    private static SortedDocValues sortedDocValues(List<BytesRef> values) {
        if (values.isEmpty()) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedDocValues() {
            @Override
            public int ordValue() {
                return 0;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return values.get(0);
            }

            @Override
            public int getValueCount() {
                return values.size();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    private static SortedNumericDocValues sortedNumericDocValues(List<Long> values) {
        if (values.isEmpty()) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedNumericDocValues() {

            int i = -1;

            @Override
            public long nextValue() {
                i++;
                return values.get(i);
            }

            @Override
            public int docValueCount() {
                return values.size();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                i = -1;
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                i = -1;
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                i = -1;
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }

    private static SortedSetDocValues sortedSetDocValues(List<BytesRef> distinct) {
        if (distinct.isEmpty()) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedSetDocValues() {

            int i = -1;

            @Override
            public long nextOrd() {
                i++;
                assert i < distinct.size();
                return i;
            }

            @Override
            public int docValueCount() {
                return distinct.size();
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return distinct.get((int) ord);
            }

            @Override
            public long getValueCount() {
                return distinct.size();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                i = -1;
                return disi.advance(target) == target;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                i = -1;
                return disi.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                i = -1;
                return disi.advance(target);
            }

            @Override
            public long cost() {
                return disi.cost();
            }
        };
    }
}
