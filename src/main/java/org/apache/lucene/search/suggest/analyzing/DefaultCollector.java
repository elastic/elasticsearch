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

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Default collector implementation
 *
 */
public class DefaultCollector<W> implements SegmentLookup.Collector<W> {

    private final List<Result<W>> results;
    private final FieldValues fieldValues;

    /**
     * Default collector to collect only output, score and docId for every inner result
     */
    public DefaultCollector() {
        this.results = new ArrayList<>();
        this.fieldValues = null;
    }

    /**
     * Default collector to collect output, score, docId and <code>fields</code>
     * from the index for every inner result
     * @param reader to be used to retrieve field values
     * @param fields set of field names (fields can be stored fields or doc values)
     * @throws IOException
     */
    public DefaultCollector(LeafReader reader, Set<String> fields) throws IOException {
        if (reader == null) {
            throw new IllegalArgumentException("reader can not be null");
        }
        if (fields == null) {
            throw new IllegalArgumentException("fields can not be null");
        }
        this.results = new ArrayList<>();
        this.fieldValues = new FieldValues(reader, fields);
    }

    @Override
    public void collect(CharSequence key, List<ResultMetaData<W>> resultMetaDataList) throws IOException {
        List<Result.InnerResult<W>> innerResults = new ArrayList<>(resultMetaDataList.size());
        for (ResultMetaData<W> resultMetaData : resultMetaDataList) {
            if (fieldValues == null) {
                innerResults.add(new Result.InnerResult<>(resultMetaData));
            } else {
                innerResults.add(new Result.InnerResult<>(resultMetaData, fieldValues.get(resultMetaData.docId())));
            }
        }
        this.results.add(new Result<>(key, innerResults));
    }

    public List<Result<W>> get() {
        return results;
    }


    public static class Result<W> {
        private final CharSequence key;
        private final List<InnerResult<W>> innerResults;

        public static final class InnerResult<W> {
            final ResultMetaData<W> resultMetaData;
            final List<FieldValue> fieldValues;

            InnerResult(ResultMetaData<W> resultMetaData, List<FieldValue> fieldValues) {
                this.resultMetaData = resultMetaData;
                this.fieldValues = fieldValues;
            }

            InnerResult(ResultMetaData<W> resultMetaData) {
                this.resultMetaData = resultMetaData;
                this.fieldValues = null;
            }

            public List<FieldValue> fieldValues() {
                return fieldValues;
            }

            public W score() {
                return resultMetaData.score();
            }

            public CharSequence output() {
                return resultMetaData.output();
            }

            public int docID() {
                return resultMetaData.docId();
            }
        }

        Result(CharSequence key, List<InnerResult<W>> innerResults) {
            this.key = key;
            this.innerResults = innerResults;
        }

        /**
         * Returns the analyzed form of the lookup field value
         */
        public CharSequence key() {
            return key;
        }

        /**
         * Returns a list of (output (lookup field value), score and weight)
         * along with {@link org.apache.lucene.search.suggest.analyzing.DefaultCollector.FieldValue}s
         * of any field(s) if specified
         * for the current key ordered by the specified score
         */
        public List<InnerResult<W>> innerResults() {
            return innerResults;
        }

    }

    /**
     * Generic interface for accessing field values (can be from stored fields or doc values)
     */
    public interface FieldValue {

        /**
         * Name of the field
         */
        public String name();

        /**
         * True if any of the field value(s) is {@link java.lang.Number}
         * can be int, float, long or double
         */
        public boolean isNumeric();

        /**
         * True if any of the field value(s) is {@link org.apache.lucene.util.BytesRef}
         */
        public boolean isBinary();

        /**
         * True if any of the field value(s) is {@link java.lang.String}
         */
        public boolean isString();

        /**
         * Returns a List of {@link org.apache.lucene.util.BytesRef} for a field
         * or <code>null</code> if the field has no binary values
         */
        public List<BytesRef> binaryValues();

        /**
         * Returns a List of {@link String} for a field
         * or <code>null</code> if the field has no string values
         */
        public List<String> stringValues();

        /**
         * Returns a List of {@link Number} for a field
         * or <code>null</code> if the field has no numeric values
         */
        public List<Number> numericValues();
    }

    /**
     * {@link org.apache.lucene.search.suggest.analyzing.DefaultCollector.FieldValue} implementation for doc values
     */
    private final class DocValueField implements FieldValue {
        final String name;
        final BytesRef[] bytesValues;
        final Number[] longValues;

        public DocValueField(String name, Number... values) {
            this(name, values, null);
        }

        public DocValueField(String name, BytesRef... values) {
            this(name, null, values);
        }

        private DocValueField(String name, Number[] longValues, BytesRef[] bytesValues) {
            this.name = name;
            this.longValues = longValues;
            this.bytesValues = bytesValues;
            assert (longValues != null && bytesValues == null) || (longValues == null && bytesValues != null) : "only one of long value or bytes value should be set";
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isNumeric() {
            return longValues != null;
        }

        @Override
        public boolean isBinary() {
            return bytesValues != null;
        }

        @Override
        public boolean isString() {
            return false;
        }

        @Override
        public List<BytesRef> binaryValues() {
            if (bytesValues == null) {
                throw new IllegalStateException("field: "+ name + "does not contain any bytesRef values");
            }
            return Arrays.asList(bytesValues);
        }

        @Override
        public List<String> stringValues() {
            return null;
        }

        @Override
        public List<Number> numericValues() {
            if (longValues == null) {
                throw new IllegalStateException("field: "+ name + "does not contain any number values");
            }
            return Arrays.asList(longValues);
        }
    }

    /**
     * {@link org.apache.lucene.search.suggest.analyzing.DefaultCollector.FieldValue} implementation
     * for stored fields
     */
    private final class StoredField implements FieldValue {

        Object[] values;
        final int numValues;
        int index = -1;
        public final String name;

        StoredField(String name, IndexableField[] fields) {
            this.name = name;
            this.numValues = fields.length;
            this.values = new Object[numValues];

            for (IndexableField field : fields) {
                boolean valueAdded = add(field.numericValue()) || add(field.binaryValue()) || add(field.stringValue());
                if (!valueAdded) {
                    throw new UnsupportedOperationException("Field: " + name + " has to be of string or binary or number type");
                }
            }
        }

        private boolean add(Object value) {
            if (value == null) {
                return false;
            }
            if (++index < numValues) {
                this.values[index] = value;
            } else {
                assert false : "Object array size=" + numValues + " attempting to add " + index + " items";
            }
            return true;
        }

        /**
         * Returns a List of {@link Object} for a stored field
         */
        public Object[] getValues() {
            assert index == numValues - 1;
            return values;
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder("<" + name + ":[");
            for (int i=0; i < values.length; i++) {
                stringBuilder.append(values[i].toString());
                if (i != values.length-1) {
                    stringBuilder.append(", ");
                }
            }
            stringBuilder.append("]");
            return stringBuilder.toString();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isNumeric() {
            for (int i = 0; i < numValues; i++) {
                if (values[i] instanceof Number) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isBinary() {
            for (int i = 0; i < numValues; i++) {
                if (values[i] instanceof BytesRef) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isString() {
            for (int i = 0; i < numValues; i++) {
                if (values[i] instanceof String) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public List<BytesRef> binaryValues() {
            List<BytesRef> binaryValues = null;
            for (Object value : values) {
                if (value instanceof BytesRef) {
                    if (binaryValues == null) {
                        binaryValues = new ArrayList<>(numValues);
                    }
                    binaryValues.add((BytesRef) value);
                }
            }
            return binaryValues;
        }

        @Override
        public List<String> stringValues() {
            List<String> stringValues = null;
            for (Object value : values) {
                if (value instanceof String) {
                    if (stringValues == null) {
                        stringValues = new ArrayList<>(numValues);
                    }
                    stringValues.add((String) value);
                }
            }
            return stringValues;
        }

        @Override
        public List<Number> numericValues() {
            List<Number> numericValues = null;
            for (Object value : values) {
                if (value instanceof Number) {
                    if (numericValues == null) {
                        numericValues = new ArrayList<>(numValues);
                    }
                    numericValues.add((Number) value);
                }
            }
            return numericValues;
        }
    }

    private class FieldValues {
        private final LeafReader reader;

        // lazy load
        private Set<String> storedFields;
        private List<Entry<String, NumericDocValues>> numericDocValues;
        private List<Entry<String, BinaryDocValues>> binaryDocValues;
        private List<Entry<String, SortedDocValues>> sortedDocValues;
        private List<Entry<String, SortedNumericDocValues>> sortedNumericDocValues;
        private List<Entry<String, SortedSetDocValues>> sortedSetDocValues;

        public FieldValues(LeafReader reader, Set<String> fields) throws IOException {
            this.reader = reader;
            FieldInfos fieldInfos = reader.getFieldInfos();
            for (String field : fields) {
                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
                if (fieldInfo != null) {
                    if (fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                        if (storedFields == null) {
                            storedFields = new HashSet<>(fields.size());
                        }
                        storedFields.add(field);
                    } else {
                        addDocValues(fieldInfo.getDocValuesType(), field);
                    }
                }
            }
        }

        private void addDocValues(DocValuesType type, String field) throws IOException {
            switch (type) {
                case NONE:
                    break;
                case NUMERIC:
                    if (numericDocValues == null) {
                        numericDocValues = new ArrayList<>();
                    }
                    numericDocValues.add(new AbstractMap.SimpleEntry<>(field, DocValues.getNumeric(reader, field)));
                    break;
                case BINARY:
                    if (binaryDocValues == null) {
                        binaryDocValues = new ArrayList<>();
                    }
                    binaryDocValues.add(new AbstractMap.SimpleEntry<>(field, DocValues.getBinary(reader, field)));
                    break;
                case SORTED:
                    if (sortedDocValues == null) {
                        sortedDocValues = new ArrayList<>();
                    }
                    sortedDocValues.add(new AbstractMap.SimpleEntry<>(field, DocValues.getSorted(reader, field)));
                    break;
                case SORTED_NUMERIC:
                    if (sortedNumericDocValues == null) {
                        sortedNumericDocValues = new ArrayList<>();
                    }
                    sortedNumericDocValues.add(new AbstractMap.SimpleEntry<>(field, DocValues.getSortedNumeric(reader, field)));
                    break;
                case SORTED_SET:
                    if (sortedSetDocValues == null) {
                        sortedSetDocValues = new ArrayList<>();
                    }
                    sortedSetDocValues.add(new AbstractMap.SimpleEntry<>(field, DocValues.getSortedSet(reader, field)));
                    break;
            }
        }

        public List<List<FieldValue>> get(int[] docIDs) throws IOException {
            List<List<FieldValue>> results = new ArrayList<>(docIDs.length);
            for (int docID : docIDs) {
                results.add(get(docID));
            }
            return results;
        }

        private List<FieldValue> get(int docID) throws IOException {
            List<FieldValue> resultForDocID = new ArrayList<>();
            if (storedFields != null) {
                Document document = reader.document(docID, storedFields);
                for (String storedField : storedFields) {
                    resultForDocID.add(new StoredField(storedField, document.getFields(storedField)));
                }
            }

            if (numericDocValues != null) {
                for (Entry<String, NumericDocValues> numericDocValue : numericDocValues) {
                    String field = numericDocValue.getKey();
                    NumericDocValues value = numericDocValue.getValue();
                    resultForDocID.add(new DocValueField(field, value.get(docID)));
                }
            }

            if (binaryDocValues != null) {
                for (Entry<String, BinaryDocValues> binaryDocValue : binaryDocValues) {
                    String field = binaryDocValue.getKey();
                    BinaryDocValues value = binaryDocValue.getValue();
                    resultForDocID.add(new DocValueField(field, BytesRef.deepCopyOf(value.get(docID))));
                }
            }

            if (sortedDocValues != null) {
                for (Entry<String, SortedDocValues> sortedDocValue : sortedDocValues) {
                    String field = sortedDocValue.getKey();
                    SortedDocValues value = sortedDocValue.getValue();
                    resultForDocID.add(new DocValueField(field, BytesRef.deepCopyOf(value.get(docID))));
                }
            }

            if (sortedNumericDocValues != null) {
                for (Entry<String, SortedNumericDocValues> sortedNumericDocValue : sortedNumericDocValues) {
                    String field = sortedNumericDocValue.getKey();
                    SortedNumericDocValues value = sortedNumericDocValue.getValue();
                    value.setDocument(docID);
                    Number[] fieldValues = new Number[value.count()];
                    for (int i = 0; i < value.count(); i++) {
                        fieldValues[i] = value.valueAt(i);
                    }
                    resultForDocID.add(new DocValueField(field, fieldValues));
                }
            }

            if (sortedSetDocValues != null) {
                for (Entry<String, SortedSetDocValues> sortedSetDocValue : sortedSetDocValues) {
                    String field = sortedSetDocValue.getKey();
                    SortedSetDocValues value = sortedSetDocValue.getValue();
                    List<BytesRef> fieldValues = new ArrayList<>();
                    value.setDocument(docID);
                    long ord;
                    while ((ord = value.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                        fieldValues.add(BytesRef.deepCopyOf(value.lookupOrd(ord))) ;
                    }
                    resultForDocID.add(new DocValueField(field, fieldValues.toArray(new BytesRef[fieldValues.size()])));
                }
            }
            return resultForDocID;
        }
    }
}
