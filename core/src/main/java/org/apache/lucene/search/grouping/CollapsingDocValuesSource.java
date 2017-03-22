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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Utility class that ensures that a single collapse key is extracted per document.
 */
abstract class CollapsingDocValuesSource<T> {
    protected final String field;

    CollapsingDocValuesSource(String field) throws IOException {
        this.field = field;
    }

    abstract T get(int doc);

    abstract T copy(T value, T reuse);

    abstract void setNextReader(LeafReader reader) throws IOException;

    /**
     * Implementation for {@link NumericDocValues} and {@link SortedNumericDocValues}.
     * Fails with an {@link IllegalStateException} if a document contains multiple values for the specified field.
     */
    static class Numeric extends CollapsingDocValuesSource<Long> {
        private NumericDocValues values;
        private Bits docsWithField;

        Numeric(String field) throws IOException {
            super(field);
        }

        @Override
        public Long get(int doc) {
            if (docsWithField.get(doc)) {
                return values.get(doc);
            } else {
                return null;
            }
        }

        @Override
        public Long copy(Long value, Long reuse) {
            return value;
        }

        @Override
        public void setNextReader(LeafReader reader) throws IOException {
            DocValuesType type = getDocValuesType(reader, field);
            if (type == null || type == DocValuesType.NONE) {
                values = DocValues.emptyNumeric();
                docsWithField = new Bits.MatchNoBits(reader.maxDoc());
                return ;
            }
            docsWithField = DocValues.getDocsWithField(reader, field);
            switch (type) {
                case NUMERIC:
                    values = DocValues.getNumeric(reader, field);
                    break;

                case SORTED_NUMERIC:
                    final SortedNumericDocValues sorted = DocValues.getSortedNumeric(reader, field);
                    values = DocValues.unwrapSingleton(sorted);
                    if (values == null) {
                        values = new NumericDocValues() {
                            @Override
                            public long get(int docID) {
                                sorted.setDocument(docID);
                                assert sorted.count() > 0;
                                if (sorted.count() > 1) {
                                    throw new IllegalStateException("failed to collapse " + docID +
                                        ", the collapse field must be single valued");
                                }
                                return sorted.valueAt(0);
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
        private Bits docsWithField;
        private SortedDocValues values;

        Keyword(String field) throws IOException {
            super(field);
        }

        @Override
        public BytesRef get(int doc) {
            if (docsWithField.get(doc)) {
                return values.get(doc);
            } else {
                return null;
            }
        }

        @Override
        public BytesRef copy(BytesRef value, BytesRef reuse) {
            if (value == null) {
                return null;
            }
            if (reuse != null) {
                reuse.bytes = ArrayUtil.grow(reuse.bytes, value.length);
                reuse.offset = 0;
                reuse.length = value.length;
                System.arraycopy(value.bytes, value.offset, reuse.bytes, 0, value.length);
                return reuse;
            } else {
                return BytesRef.deepCopyOf(value);
            }
        }

        @Override
        public void setNextReader(LeafReader reader) throws IOException {
            DocValuesType type = getDocValuesType(reader, field);
            if (type == null || type == DocValuesType.NONE) {
                values = DocValues.emptySorted();
                docsWithField = new Bits.MatchNoBits(reader.maxDoc());
                return ;
            }
            docsWithField = DocValues.getDocsWithField(reader, field);
            switch (type) {
                case SORTED:
                    values = DocValues.getSorted(reader, field);
                    break;

                case SORTED_SET:
                    final SortedSetDocValues sorted = DocValues.getSortedSet(reader, field);
                    values = DocValues.unwrapSingleton(sorted);
                    if (values == null) {
                        values = new SortedDocValues() {
                            @Override
                            public int getOrd(int docID) {
                                sorted.setDocument(docID);
                                int ord = (int) sorted.nextOrd();
                                if (sorted.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                                    throw new IllegalStateException("failed to collapse " + docID +
                                        ", the collapse field must be single valued");
                                }
                                return ord;
                            }

                            @Override
                            public BytesRef lookupOrd(int ord) {
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
