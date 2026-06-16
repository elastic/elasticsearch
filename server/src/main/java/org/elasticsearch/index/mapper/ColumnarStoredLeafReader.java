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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
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
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A {@link LeafReader} over a single {@link LuceneDocument} that serves doc values and stored fields
 * directly from the document's in-memory {@link IndexableField} list, without building a real Lucene index.
 *
 * <p>This reader is used by {@link SourceFieldMapper} in {@code columnar_stored} mode to drive
 * {@link SourceLoader.Synthetic} at index time. Only the methods called by the synthetic-source path are
 * implemented; all others throw {@link UnsupportedOperationException}.
 */
class ColumnarStoredLeafReader extends LeafReader {

    private static final FieldInfos EMPTY_FIELD_INFOS = new FieldInfos(new FieldInfo[0]) {

        @Override
        public FieldInfo fieldInfo(String fieldName) {
            // It is ok to return null here, since this will be used by DocValues#checkField(...) to return an empty doc values instance.
            return null;
        }

    };
    private final Map<String, List<IndexableField>> fieldsByName;
    // Dedicated maps for doc values types that are by nature single valued:
    private final Map<String, IndexableField> binaryDocValuesName;
    private final Map<String, IndexableField> numericDocValuesName;
    private final Map<String, IndexableField> sortedDocValuesName;

    /**
     * Constructs a reader over the given document.
     * The document's fields are grouped by name in a single pass so that subsequent accessor
     * calls are efficient.
     */
    ColumnarStoredLeafReader(LuceneDocument document) {
        this.fieldsByName = new HashMap<>(document.getFields().size());
        this.binaryDocValuesName = new HashMap<>(document.getFields().size());
        this.sortedDocValuesName = new HashMap<>(document.getFields().size());
        this.numericDocValuesName = new HashMap<>(document.getFields().size());
        for (IndexableField field : document.getFields()) {
            if (field.fieldType().docValuesType() == DocValuesType.BINARY) {
                var prev = binaryDocValuesName.put(field.name(), field);
                assert prev == null;
            } else if (field.fieldType().docValuesType() == DocValuesType.NUMERIC) {
                var prev = numericDocValuesName.put(field.name(), field);
                assert prev == null;
            } else if (field.fieldType().docValuesType() == DocValuesType.SORTED) {
                var prev = sortedDocValuesName.put(field.name(), field);
                assert prev == null;
            } else {
                fieldsByName.computeIfAbsent(field.name(), k -> new ArrayList<>(4)).add(field);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Doc-values accessors
    // -------------------------------------------------------------------------

    @Override
    public NumericDocValues getNumericDocValues(String fieldName) throws IOException {
        IndexableField field = numericDocValuesName.get(fieldName);
        if (field != null) {
            return SingleDocLeafReaderUtils.numericDocValues(field.numericValue());
        } else {
            return null;
        }
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String fieldName) throws IOException {
        IndexableField field = binaryDocValuesName.get(fieldName);
        if (field != null) {
            return SingleDocLeafReaderUtils.binaryDocValues(field.binaryValue());
        } else {
            return null;
        }
    }

    @Override
    public SortedDocValues getSortedDocValues(String fieldName) throws IOException {
        IndexableField field = sortedDocValuesName.get(fieldName);
        if (field != null) {
            return SingleDocLeafReaderUtils.sortedDocValues(field.binaryValue());
        } else {
            return null;
        }
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        List<IndexableField> fields = fieldsByName.getOrDefault(field, List.of());
        List<Number> values = new ArrayList<>();
        for (IndexableField f : fields) {
            if (f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC) {
                values.add(f.numericValue());
            }
        }
        // Lucene SortedNumericDocValues: values sorted ascending, duplicates kept.
        values.sort(null);
        return SingleDocLeafReaderUtils.sortedNumericDocValues(values);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        List<IndexableField> fields = fieldsByName.getOrDefault(field, List.of());
        // TreeSet gives sorted, deduplicated values matching Lucene's SortedSetDocValues ordinal convention.
        TreeSet<BytesRef> distinct = new TreeSet<>();
        for (IndexableField f : fields) {
            if (f.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
                distinct.add(f.binaryValue());
            }
        }
        return SingleDocLeafReaderUtils.sortedSetDocValues(new ArrayList<>(distinct));
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
                        FieldInfo fieldInfo = SingleDocLeafReaderUtils.fieldInfo(field.name());
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
        return EMPTY_FIELD_INFOS;
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

}
