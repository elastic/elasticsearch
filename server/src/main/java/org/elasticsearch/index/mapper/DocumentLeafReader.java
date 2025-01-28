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
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A {@link LeafReader} over a lucene document that exposes doc values and stored fields.
 * Note that unlike lucene's {@link MemoryIndex} implementation, this holds no state and
 * does not attempt to do any analysis on text fields.  It is used to back index-time
 * scripts that reference field data and stored fields from a document that has not yet
 * been indexed.
 */
class DocumentLeafReader extends LeafReader {

    private final LuceneDocument document;
    private final Map<String, Consumer<LeafReaderContext>> calculatedFields;
    private final Set<String> fieldPath = new LinkedHashSet<>();

    DocumentLeafReader(LuceneDocument document, Map<String, Consumer<LeafReaderContext>> calculatedFields) {
        this.document = document;
        this.calculatedFields = calculatedFields;
    }

    private void checkField(String field) {
        if (calculatedFields.containsKey(field)) {
            // this means that a mapper script is referring to another calculated field;
            // in which case we need to execute that field first. We also check for loops here
            if (fieldPath.add(field) == false) {
                throw new IllegalArgumentException("Loop in field resolution detected: " + String.join("->", fieldPath) + "->" + field);
            }
            calculatedFields.get(field).accept(this.getContext());
            fieldPath.remove(field);
        }
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        checkField(field);
        List<Number> values = document.getFields()
            .stream()
            .filter(f -> Objects.equals(f.name(), field))
            .filter(f -> f.fieldType().docValuesType() == DocValuesType.NUMERIC)
            .map(IndexableField::numericValue)
            .sorted()
            .toList();
        return numericDocValues(values);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        checkField(field);
        List<BytesRef> values = document.getFields()
            .stream()
            .filter(f -> Objects.equals(f.name(), field))
            .filter(f -> f.fieldType().docValuesType() == DocValuesType.BINARY)
            .map(IndexableField::binaryValue)
            .sorted()
            .toList();
        return binaryDocValues(values);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        checkField(field);
        List<BytesRef> values = document.getFields()
            .stream()
            .filter(f -> Objects.equals(f.name(), field))
            .filter(f -> f.fieldType().docValuesType() == DocValuesType.SORTED)
            .map(IndexableField::binaryValue)
            .sorted()
            .toList();
        return sortedDocValues(values);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        checkField(field);
        List<Number> values = document.getFields()
            .stream()
            .filter(f -> Objects.equals(f.name(), field))
            .filter(f -> f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC)
            .map(IndexableField::numericValue)
            .sorted()
            .toList();
        return sortedNumericDocValues(values);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        checkField(field);
        List<BytesRef> values = document.getFields()
            .stream()
            .filter(f -> Objects.equals(f.name(), field))
            .filter(f -> f.fieldType().docValuesType() == DocValuesType.SORTED_SET)
            .map(IndexableField::binaryValue)
            .sorted()
            .toList();
        return sortedSetDocValues(values);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return new FieldInfos(new FieldInfo[0]);
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                List<IndexableField> fields = document.getFields().stream().filter(f -> f.fieldType().stored()).toList();
                for (IndexableField field : fields) {
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
                        // We can't just pass field.binaryValue().bytes here as there may be offset/length
                        // considerations
                        byte[] data = new byte[field.binaryValue().length];
                        System.arraycopy(field.binaryValue().bytes, field.binaryValue().offset, data, 0, data.length);
                        visitor.binaryField(fieldInfo, data);
                    }
                }
            }
        };
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Terms terms(String field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) {
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
    public int maxDoc() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doClose() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TermVectors termVectors() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        throw new UnsupportedOperationException();
    }

    // Our StoredFieldsVisitor implementations only check the name of the passed-in
    // FieldInfo, so that's the only value we need to set here.
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
        if (values.size() == 0) {
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

    private static SortedNumericDocValues sortedNumericDocValues(List<Number> values) {
        if (values.size() == 0) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedNumericDocValues() {

            int i = -1;

            @Override
            public long nextValue() {
                i++;
                return values.get(i).longValue();
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

    private static BinaryDocValues binaryDocValues(List<BytesRef> values) {
        if (values.size() == 0) {
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
        if (values.size() == 0) {
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

    private static SortedSetDocValues sortedSetDocValues(List<BytesRef> values) {
        if (values.size() == 0) {
            return null;
        }
        DocIdSetIterator disi = DocIdSetIterator.all(1);
        return new SortedSetDocValues() {

            int i = -1;

            @Override
            public long nextOrd() {
                i++;
                assert i < values.size();
                return i;
            }

            @Override
            public int docValueCount() {
                return values.size();
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return values.get((int) ord);
            }

            @Override
            public long getValueCount() {
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
}
