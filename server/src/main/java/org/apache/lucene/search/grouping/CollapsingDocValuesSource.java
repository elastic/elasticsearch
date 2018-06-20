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
package org.apache.lucene.search.grouping;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractNumericDocValues;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility class that ensures that a single collapse key is extracted per document.
 */
abstract class CollapsingDocValuesSource<T> extends GroupSelector<T> {
    @Override
    public void setGroups(Collection<SearchGroup<T>> groups) {
        throw new UnsupportedOperationException();
    }

    /**
     * Implementation for {@link SortedDocValues} and {@link SortedSetDocValues}.
     * Fails with an {@link IllegalStateException} if a document contains multiple values for of the collapsed fields.
     */
    static class MultipleKeyword extends CollapsingDocValuesSource<List<BytesRef>> {
        private final String[] fields;
        private final int fieldsCount;
        private SortedDocValues[] values;
        private int[] ord;
        private boolean valuesExist;

        MultipleKeyword(String[] fields) {
            this.fields = fields;
            this.fieldsCount = fields.length;
            this.values = new SortedDocValues[fieldsCount];
            this.ord = new int[fieldsCount];
        }

        @Override
        public org.apache.lucene.search.grouping.GroupSelector.State advanceTo(int doc)
            throws IOException {
            valuesExist = true;
            for (int i = 0;  i < fieldsCount; i++) {
                if (values[i].advanceExact(doc)) {
                    ord[i] = values[i].ordValue();
                } else {
                    valuesExist = false;
                    return State.SKIP;
                }
            }
            return State.ACCEPT;
        }

        @Override
        public List<BytesRef> currentValue() {
            if (valuesExist == false) {
                return null;
            } else {
                try {
                    ArrayList<BytesRef> result = new ArrayList<>();
                    for (int i = 0;  i < fieldsCount; i++) {
                        result.add(values[i].lookupOrd(ord[i]));
                    }
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public List<BytesRef> copyValue() {
            if (valuesExist == false) {
                return null;
            } else {
                try {
                    ArrayList<BytesRef> result = new ArrayList<>();
                    for (int i = 0;  i < fieldsCount; i++) {
                        BytesRef value = values[i].lookupOrd(ord[i]);
                        result.add(BytesRef.deepCopyOf(value));
                    }
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) throws IOException {
            LeafReader reader = readerContext.reader();
            for (int i = 0;  i < fieldsCount; i++) {
                DocValuesType type = getDocValuesType(reader, fields[i]);
                if (type == null || type == DocValuesType.NONE) {
                    values[i] = DocValues.emptySorted();
                    continue;
                }
                switch (type) {
                    case SORTED:
                        values[i] = DocValues.getSorted(reader, fields[i]);
                        break;
                    case SORTED_SET:
                        final SortedSetDocValues sorted = DocValues.getSortedSet(reader, fields[i]);
                        values[i] = DocValues.unwrapSingleton(sorted);
                        if (values[i] == null) {
                            values[i] = new AbstractSortedDocValues() {
                                private int ord;
                                @Override
                                public boolean advanceExact(int target) throws IOException {
                                    if (sorted.advanceExact(target)) {
                                        ord = (int) sorted.nextOrd();
                                        if (sorted.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                                            throw new IllegalStateException("failed to collapse " + target +
                                                ", the collapse field must be single valued");
                                        }
                                        return true;
                                    } else {
                                        return false;
                                    }
                                }
                                @Override
                                public int docID() {
                                    return sorted.docID();
                                }
                                @Override
                                public int ordValue() {
                                    return ord;
                                }
                                @Override
                                public BytesRef lookupOrd(int ord) throws IOException {
                                    return sorted.lookupOrd(ord);
                                }
                                @Override
                                public int getValueCount() {
                                    return (int) sorted.getValueCount();
                                }
                            };
                        }
                        break;
                    default:
                        throw new IllegalStateException("unexpected doc values type [" + type + "] for field ]" + fields[i] + "]");
                }
            }
        }
    }


    /**
     * Implementation for {@link NumericDocValues} and {@link SortedNumericDocValues}.
     * Fails with an {@link IllegalStateException} if a document contains multiple values for one of the collapsed fields.
     */
    static class MultipleNumeric extends CollapsingDocValuesSource<List<Long>> {
        private final String[] fields;
        private final int fieldsCount;
        private NumericDocValues[] docValues;
        private boolean valuesExist;
        private List<Long> values;

        MultipleNumeric(String[] fields) {
            this.fields = fields;
            this.fieldsCount = fields.length;
            this.docValues = new NumericDocValues[fieldsCount];
        }

        @Override
        public org.apache.lucene.search.grouping.GroupSelector.State advanceTo(int doc)
            throws IOException {
            valuesExist = true;
            values = new ArrayList<>(fieldsCount);
            for (int i = 0;  i < fieldsCount; i++) {
                if (docValues[i].advanceExact(doc)) {
                    values.add(docValues[i].longValue());
                } else {
                    valuesExist = false;
                    return State.SKIP;
                }
            }
            return State.ACCEPT;
        }

        @Override
        public List<Long> currentValue() {
            if (valuesExist == false) {
                return null;
            } else {
               return values;
            }
        }

        @Override
        public List<Long> copyValue() {
            return currentValue();
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) throws IOException {
            LeafReader reader = readerContext.reader();
            for (int i = 0;  i < fieldsCount; i++) {
                DocValuesType type = getDocValuesType(reader, fields[i]);
                if (type == null || type == DocValuesType.NONE) {
                    docValues[i] = DocValues.emptyNumeric();
                    continue;
                }
                switch (type) {
                    case NUMERIC:
                        docValues[i] = DocValues.getNumeric(reader, fields[i]);
                        break;
                    case SORTED_NUMERIC:
                        final SortedNumericDocValues sorted = DocValues.getSortedNumeric(reader, fields[i]);
                        docValues[i] = DocValues.unwrapSingleton(sorted);
                        if (docValues[i] == null) {
                            docValues[i] = new AbstractNumericDocValues() {
                                private long value;
                                @Override
                                public boolean advanceExact(int target) throws IOException {
                                    if (sorted.advanceExact(target)) {
                                        if (sorted.docValueCount() > 1) {
                                            throw new IllegalStateException("failed to collapse " + target +
                                                ", the collapse field must be single valued");
                                        }
                                        value = sorted.nextValue();
                                        return true;
                                    } else {
                                        return false;
                                    }
                                }
                                @Override
                                public int docID() {
                                    return sorted.docID();
                                }
                                @Override
                                public long longValue() throws IOException {
                                    return value;
                                }
                            };
                        }
                        break;
                    default:
                        throw new IllegalStateException("unexpected doc values type [" + type + "] for field [" + fields[i] + "]");
                }
            }
        }
    }


    private static DocValuesType getDocValuesType(LeafReader in, String field) {
        FieldInfo fi = in.getFieldInfos().fieldInfo(field);
        if (fi != null) {
            return fi.getDocValuesType();
        }
        return null;
    }
}
