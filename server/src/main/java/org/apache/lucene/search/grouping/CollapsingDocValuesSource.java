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
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Collection;

/**
 * Utility class that ensures that a single collapse key is extracted per document.
 */
abstract class CollapsingDocValuesSource<T> extends GroupSelector<T> {
    protected final String field;

    CollapsingDocValuesSource(String field) {
        this.field = field;
    }

    @Override
    public void setGroups(Collection<SearchGroup<T>> groups) {
        throw new UnsupportedOperationException();
    }

    /**
     * Implementation for {@link NumericDocValues} and {@link SortedNumericDocValues}.
     * Fails with an {@link IllegalStateException} if a document contains multiple values for the specified field.
     */
    static class Numeric extends CollapsingDocValuesSource<Long> {
        private NumericDocValues values;
        private long value;
        private boolean hasValue;

        Numeric(MappedFieldType fieldType) {
            super(fieldType.name());
        }

        @Override
        public State advanceTo(int doc) throws IOException {
            if (values.advanceExact(doc)) {
                hasValue = true;
                value = values.longValue();
                return State.ACCEPT;
            } else {
                hasValue = false;
                return State.SKIP;
            }
        }

        @Override
        public Long currentValue() {
            return hasValue ? value : null;
        }

        @Override
        public Long copyValue() {
            return currentValue();
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) throws IOException {
            LeafReader reader = readerContext.reader();
            DocValuesType type = getDocValuesType(reader, field);
            if (type == null || type == DocValuesType.NONE) {
                values = DocValues.emptyNumeric();
                return ;
            }
            switch (type) {
                case NUMERIC:
                    values = DocValues.getNumeric(reader, field);
                    break;

                case SORTED_NUMERIC:
                    final SortedNumericDocValues sorted = DocValues.getSortedNumeric(reader, field);
                    values = DocValues.unwrapSingleton(sorted);
                    if (values == null) {
                        values = new AbstractNumericDocValues() {

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
                    throw new IllegalStateException("unexpected doc values type " +
                        type + "` for field `" + field + "`");
            }
        }
    }

    /**
     * Implementation for {@link SortedDocValues} and {@link SortedSetDocValues}.
     * Fails with an {@link IllegalStateException} if a document contains multiple values for the specified field.
     */
    static class Keyword extends CollapsingDocValuesSource<BytesRef> {
        private SortedDocValues values;
        private int ord;

        Keyword(MappedFieldType fieldType) {
            super(fieldType.name());
        }

        @Override
        public org.apache.lucene.search.grouping.GroupSelector.State advanceTo(int doc)
                throws IOException {
            if (values.advanceExact(doc)) {
                ord = values.ordValue();
                return State.ACCEPT;
            } else {
                ord = -1;
                return State.SKIP;
            }
        }

        @Override
        public BytesRef currentValue() {
            if (ord == -1) {
                return null;
            } else {
                try {
                    return values.lookupOrd(ord);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public BytesRef copyValue() {
            BytesRef value = currentValue();
            if (value == null) {
                return null;
            } else {
                return BytesRef.deepCopyOf(value);
            }
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) throws IOException {
            LeafReader reader = readerContext.reader();
            DocValuesType type = getDocValuesType(reader, field);
            if (type == null || type == DocValuesType.NONE) {
                values = DocValues.emptySorted();
                return ;
            }
            switch (type) {
                case SORTED:
                    values = DocValues.getSorted(reader, field);
                    break;

                case SORTED_SET:
                    final SortedSetDocValues sorted = DocValues.getSortedSet(reader, field);
                    values = DocValues.unwrapSingleton(sorted);
                    if (values == null) {
                        values = new AbstractSortedDocValues() {

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
                    throw new IllegalStateException("unexpected doc values type "
                        + type + "` for field `" + field + "`");
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
