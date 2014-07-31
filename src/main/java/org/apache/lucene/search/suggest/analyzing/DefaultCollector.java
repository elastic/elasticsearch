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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

/**
 * Default collector implementation
 *
 * TODO: add storedField and docValues lookup support support
 * NOTE: WIP
 */
public class DefaultCollector<W> implements SegmentLookup.Collector<W> {

    private final List<Result<W>> results;
    private final LeafReader reader;
    private final Set<FieldInfo> fieldInfos;
    private final Set<String> storedFields;

    public DefaultCollector() {
        this.results = new ArrayList<>();
        this.reader = null;
        this.fieldInfos = null;
        this.storedFields = null;
    }

    public DefaultCollector(LeafReader reader, Set<String> fields) {
        if (reader == null) {
            throw new IllegalArgumentException("reader can not be null");
        }
        if (fields == null) {
            throw new IllegalArgumentException("fields can not be null");
        }
        this.results = new ArrayList<>();
        this.reader = reader;
        this.storedFields = new HashSet<>(fields.size());
        this.fieldInfos = new HashSet<>(fields.size());
        //TODO: figure out what field name is a docValue/storedField from FieldInfo
        for (String field : fields) {
            FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
            if (fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                storedFields.add(field);
            }
            fieldInfos.add(fieldInfo);
        }
    }

    @Override
    public void collect(CharSequence key, List<ResultMetaData<W>> resultMetaDataList) throws IOException {
        List<Map.Entry<ResultMetaData<W>, List<StoredField>>> innerResults = new ArrayList<>(resultMetaDataList.size());
        for (ResultMetaData<W> resultMetaData : resultMetaDataList) {
            if (reader == null || fieldInfos == null) {
                innerResults.add(new AbstractMap.SimpleEntry<ResultMetaData<W>, List<StoredField>>(resultMetaData, null));
            } else {
                Document document = reader.document(resultMetaData.docId(), storedFields);
                List<StoredField> storedFieldValues = new ArrayList<>(storedFields.size());
                for (String field : storedFields) {
                    storedFieldValues.add(new StoredField(field, document.getFields(field)));
                }
                innerResults.add(new AbstractMap.SimpleEntry<>(resultMetaData, storedFieldValues));
            }
        }
        this.results.add(new Result<>(key, innerResults));
    }

    public List<Result<W>> get() {
        return results;
    }

    public class Result<W> {
        private final CharSequence key;
        private final List<Map.Entry<ResultMetaData<W>, List<StoredField>>> innerResults;

        public Result(CharSequence key, List<Map.Entry<ResultMetaData<W>, List<StoredField>>> innerResults) {
            this.key = key;
            this.innerResults = innerResults;
        }

        public CharSequence key() {
            return key;
        }

        public List<ResultMetaData<W>> resultMetaDataList() {
            List<ResultMetaData<W>> results = new ArrayList<>(innerResults.size());
            for (Map.Entry<ResultMetaData<W>, List<StoredField>> innerResult : innerResults) {
                results.add(innerResult.getKey());
            }
            return results;
        }

        public List<Map.Entry<ResultMetaData<W>, List<StoredField>>> innerResults() {
            return innerResults;
        }
    }
    // TODO:: add stored field support + docValues support
    public final class StoredField {

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
         * Returns a List of {@link Number} for a stored field
         * or <code>null</code> if the stored field has no numeric values
         */
        public List<Number> getNumericValues() {
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

        /**
         * Returns a List of {@link String} for a stored field
         * or <code>null</code> if the stored field has no string values
         */
        public List<String> getStringValues() {
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

        /**
         * Returns a List of {@link org.apache.lucene.util.BytesRef} for a stored field
         * or <code>null</code> if the stored field has no binary values
         */
        public List<BytesRef> getBinaryValues() {
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
    }
}
