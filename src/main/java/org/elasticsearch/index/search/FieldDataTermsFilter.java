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
package org.elasticsearch.index.search;

import java.io.IOException;
import java.util.Objects;

import com.carrotsearch.hppc.DoubleOpenHashSet;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.ObjectOpenHashSet;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

/**
 * Similar to a {@link org.apache.lucene.queries.TermsFilter} but pulls terms from the fielddata.
 */
public abstract class FieldDataTermsFilter extends Filter {

    final IndexFieldData fieldData;

    protected FieldDataTermsFilter(IndexFieldData fieldData) {
        this.fieldData = fieldData;
    }

    /**
     * Get a {@link FieldDataTermsFilter} that filters on non-numeric terms found in a hppc {@link ObjectOpenHashSet} of
     * {@link BytesRef}.
     *
     * @param fieldData The fielddata for the field.
     * @param terms     An {@link ObjectOpenHashSet} of terms.
     * @return the filter.
     */
    public static FieldDataTermsFilter newBytes(IndexFieldData fieldData, ObjectOpenHashSet<BytesRef> terms) {
        return new BytesFieldDataFilter(fieldData, terms);
    }

    /**
     * Get a {@link FieldDataTermsFilter} that filters on non-floating point numeric terms found in a hppc
     * {@link LongOpenHashSet}.
     *
     * @param fieldData The fielddata for the field.
     * @param terms     A {@link LongOpenHashSet} of terms.
     * @return the filter.
     */
    public static FieldDataTermsFilter newLongs(IndexNumericFieldData fieldData, LongOpenHashSet terms) {
        return new LongsFieldDataFilter(fieldData, terms);
    }

    /**
     * Get a {@link FieldDataTermsFilter} that filters on floating point numeric terms found in a hppc
     * {@link DoubleOpenHashSet}.
     *
     * @param fieldData The fielddata for the field.
     * @param terms     A {@link DoubleOpenHashSet} of terms.
     * @return the filter.
     */
    public static FieldDataTermsFilter newDoubles(IndexNumericFieldData fieldData, DoubleOpenHashSet terms) {
        return new DoublesFieldDataFilter(fieldData, terms);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (super.equals(obj) == false) return false;

        FieldDataTermsFilter that = (FieldDataTermsFilter) obj;
        if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int h = super.hashCode();
        h = 31 * h + fieldData.getFieldNames().indexName().hashCode();
        return h;
    }

    /**
     * Filters on non-numeric fields.
     */
    protected static class BytesFieldDataFilter extends FieldDataTermsFilter {

        final ObjectOpenHashSet<BytesRef> terms;

        protected BytesFieldDataFilter(IndexFieldData fieldData, ObjectOpenHashSet<BytesRef> terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (super.equals(obj) == false) {
                return false;
            }
            return Objects.equals(terms, ((BytesFieldDataFilter) obj).terms);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Objects.hashCode(terms);
        }

        @Override
        public String toString(String field) {
            final StringBuilder sb = new StringBuilder("BytesFieldDataFilter:");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "")
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.isEmpty()) return null;

            final SortedBinaryDocValues values = fieldData.load(context).getBytesValues(); // load fielddata
            return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
                @Override
                protected boolean matchDoc(int doc) {
                    values.setDocument(doc);
                    final int numVals = values.count();
                    for (int i = 0; i < numVals; i++) {
                        if (terms.contains(values.valueAt(i))) {
                            return true;
                        }
                    }

                    return false;
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }
            };
        }
    }

    /**
     * Filters on non-floating point numeric fields.
     */
    protected static class LongsFieldDataFilter extends FieldDataTermsFilter {

        final LongOpenHashSet terms;

        protected LongsFieldDataFilter(IndexNumericFieldData fieldData, LongOpenHashSet terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (super.equals(obj) == false) {
                return false;
            }
            return Objects.equals(terms, ((BytesFieldDataFilter) obj).terms);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Objects.hashCode(terms);
        }

        @Override
        public String toString(String field) {
            final StringBuilder sb = new StringBuilder("LongsFieldDataFilter:");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "")
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.isEmpty()) return null;

            IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
            if (!numericFieldData.getNumericType().isFloatingPoint()) {
                final SortedNumericDocValues values = numericFieldData.load(context).getLongValues(); // load fielddata
                return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
                    @Override
                    protected boolean matchDoc(int doc) {
                        values.setDocument(doc);
                        final int numVals = values.count();
                        for (int i = 0; i < numVals; i++) {
                            if (terms.contains(values.valueAt(i))) {
                                return true;
                            }
                        }

                        return false;
                    }
                };
            }

            // only get here if wrong fielddata type in which case
            // no docs will match so we just return null.
            return null;
        }
    }

    /**
     * Filters on floating point numeric fields.
     */
    protected static class DoublesFieldDataFilter extends FieldDataTermsFilter {

        final DoubleOpenHashSet terms;

        protected DoublesFieldDataFilter(IndexNumericFieldData fieldData, DoubleOpenHashSet terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (super.equals(obj) == false) {
                return false;
            }
            return Objects.equals(terms, ((BytesFieldDataFilter) obj).terms);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Objects.hashCode(terms);
        }

        @Override
        public String toString(String field) {
            final StringBuilder sb = new StringBuilder("DoublesFieldDataFilter");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "")
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.isEmpty()) return null;

            // verify we have a floating point numeric fielddata
            IndexNumericFieldData indexNumericFieldData = (IndexNumericFieldData) fieldData;
            if (indexNumericFieldData.getNumericType().isFloatingPoint()) {
                final SortedNumericDoubleValues values = indexNumericFieldData.load(context).getDoubleValues(); // load fielddata
                return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
                    @Override
                    protected boolean matchDoc(int doc) {
                        values.setDocument(doc);
                        final int numVals = values.count();

                        for (int i = 0; i < numVals; i++) {
                            if (terms.contains(values.valueAt(i))) {
                                return true;
                            }
                        }

                        return false;
                    }
                };
            }

            // only get here if wrong fielddata type in which case
            // no docs will match so we just return null.
            return null;
        }
    }
}
