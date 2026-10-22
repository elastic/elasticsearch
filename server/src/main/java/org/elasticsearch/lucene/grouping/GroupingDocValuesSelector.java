/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.grouping;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.AbstractNumericDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Collection;

/**
 * Utility class that ensures that a single grouping key is extracted per document.
 */
abstract class GroupingDocValuesSelector<T> extends GroupSelector<T> {
    protected final String field;

    GroupingDocValuesSelector(String field) {
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
    static class Numeric extends GroupingDocValuesSelector<Long> {
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
                return;
            }
            switch (type) {
                case NUMERIC -> values = DocValues.getNumeric(reader, field);
                case SORTED_NUMERIC -> {
                    final SortedNumericDocValues sorted = DocValues.getSortedNumeric(reader, field);
                    values = DocValues.unwrapSingleton(sorted);
                    if (values == null) {
                        values = new AbstractNumericDocValues() {

                            private long value;

                            @Override
                            public boolean advanceExact(int target) throws IOException {
                                if (sorted.advanceExact(target)) {
                                    if (sorted.docValueCount() > 1) {
                                        throw new IllegalArgumentException(
                                            "failed to extract doc:" + target + ", the grouping field must be single valued"
                                        );
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
                }
                default -> throw new IllegalArgumentException("unexpected doc values type " + type + "` for field `" + field + "`");
            }
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {}
    }

    /**
     * Implementation for {@link SortedDocValues}, {@link SortedSetDocValues} and binary doc values.
     * Fails with an {@link IllegalStateException} if a document contains multiple values for the specified field.
     *
     * <p>How a field's binary doc values are framed, and so which decoder reads them, is settled by the mapping.
     * {@code binaryValues} arrives already bound to that decision and is {@code null} for a field that writes no
     * binary blob.
     */
    static class Keyword extends GroupingDocValuesSelector<BytesRef> {
        private final CheckedFunction<LeafReader, SortableBinaryDocValues, IOException> binaryValues;
        private GroupValues values;
        private boolean hasValue;

        Keyword(MappedFieldType fieldType, @Nullable CheckedFunction<LeafReader, SortableBinaryDocValues, IOException> binaryValues) {
            super(fieldType.name());
            this.binaryValues = binaryValues;
        }

        @Override
        public org.apache.lucene.search.grouping.GroupSelector.State advanceTo(int doc) throws IOException {
            hasValue = values.advanceExact(doc);
            return hasValue ? State.ACCEPT : State.SKIP;
        }

        @Override
        public BytesRef currentValue() {
            if (hasValue == false) {
                return null;
            }
            try {
                return values.currentValue();
            } catch (IOException e) {
                throw new RuntimeException(e);
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
                values = ordinalValues(DocValues.emptySorted());
                return;
            }
            switch (type) {
                case SORTED -> values = ordinalValues(DocValues.getSorted(reader, field));
                case SORTED_SET -> {
                    SortedSetDocValues sortedSet = DocValues.getSortedSet(reader, field);
                    SortedDocValues singleton = DocValues.unwrapSingleton(sortedSet);
                    values = singleton != null ? ordinalValues(singleton) : setOrdinalValues(sortedSet);
                }
                case BINARY -> {
                    if (binaryValues == null) {
                        throw new IllegalArgumentException(
                            "field `" + field + "` has binary doc values but its mapping does not name their format"
                        );
                    }
                    values = binaryValues(binaryValues.apply(reader));
                }
                default -> throw new IllegalArgumentException("unexpected doc values type " + type + " for field `" + field + "`");
            }
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {}

        /**
         * A single grouping value per document, whatever the field's doc values look like underneath.
         *
         * <p>{@link #currentValue()} is read more than once per document, so it must not consume a cursor.
         */
        private interface GroupValues {
            boolean advanceExact(int doc) throws IOException;

            BytesRef currentValue() throws IOException;
        }

        private static GroupValues ordinalValues(SortedDocValues sorted) {
            return new GroupValues() {
                private int ord = -1;

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    if (sorted.advanceExact(doc)) {
                        ord = sorted.ordValue();
                        return true;
                    }
                    ord = -1;
                    return false;
                }

                @Override
                public BytesRef currentValue() throws IOException {
                    return ord == -1 ? null : sorted.lookupOrd(ord);
                }
            };
        }

        // TODO: group on the page ordinals ColumNAR can hand over, rather than on each document's bytes.
        // https://github.com/elastic/elasticsearch/issues/160993
        private static GroupValues binaryValues(SortableBinaryDocValues binary) {
            return new GroupValues() {
                private BytesRef value;

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    if (binary.advanceExact(doc) == false) {
                        value = null;
                        return false;
                    }
                    if (binary.docValueCount() > 1) {
                        throw new IllegalArgumentException("failed to extract doc:" + doc + ", the grouping field must be single valued");
                    }
                    value = binary.nextValue();
                    return true;
                }

                @Override
                public BytesRef currentValue() {
                    return value;
                }
            };
        }

        private static GroupValues setOrdinalValues(SortedSetDocValues sorted) {
            return new GroupValues() {
                private long ord = -1;

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    if (sorted.advanceExact(doc) == false) {
                        ord = -1;
                        return false;
                    }
                    if (sorted.docValueCount() > 1) {
                        throw new IllegalArgumentException("failed to extract doc:" + doc + ", the grouping field must be single valued");
                    }
                    ord = sorted.nextOrd();
                    return true;
                }

                @Override
                public BytesRef currentValue() throws IOException {
                    return ord == -1 ? null : sorted.lookupOrd(ord);
                }
            };
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
