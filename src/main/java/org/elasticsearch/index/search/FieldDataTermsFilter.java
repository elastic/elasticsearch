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

import com.carrotsearch.hppc.DoubleOpenHashSet;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.google.common.primitives.Longs;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.*;

import java.io.IOException;

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
     * Get a {@link FieldDataTermsFilter} that filters on non-numeric terms found in a
     * {@link org.elasticsearch.common.util.BytesRefHash}
     *
     * @param fieldData The fielddata for the field.
     * @param terms     A {@link org.elasticsearch.common.util.BytesRefHash} of terms.
     * @return the filter.
     */
    public static FieldDataTermsFilter newBytes(IndexFieldData fieldData, BytesRefHash terms) {
        return new HashedBytesFieldDataFilter(fieldData, terms);
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
     * Get a {@link FieldDataTermsFilter} that filters on non-floating point numeric terms found in a
     * {@link org.elasticsearch.common.util.LongHash}.
     *
     * @param fieldData The fielddata for the field.
     * @param terms     A {@link org.elasticsearch.common.util.LongHash} of terms.
     * @return the filter.
     */
    public static FieldDataTermsFilter newLongs(IndexNumericFieldData fieldData, LongHash terms) {
        return new HashedLongsFieldDataFilter(fieldData, terms);
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

    /**
     * Get a {@link FieldDataTermsFilter} that filters on floating point numeric terms found in a
     * {@link org.elasticsearch.common.util.LongHash}.  Terms must be represented as long bits, ie. using
     * Double.doubleToLongBits.
     *
     * @param fieldData The fielddata for the field.
     * @param terms     A {@link org.elasticsearch.common.util.LongHash} of terms.
     * @return the filter.
     */
    public static FieldDataTermsFilter newDoubles(IndexNumericFieldData fieldData, LongHash terms) {
        return new HashedDoublesFieldDataFilter(fieldData, terms);
    }

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public abstract String toString();

    /**
     * Filters on non-numeric fields.
     */
    protected static class BytesFieldDataFilter extends FieldDataTermsFilter {

        final ObjectOpenHashSet<BytesRef> terms;
        Integer hashCode;

        protected BytesFieldDataFilter(IndexFieldData fieldData, ObjectOpenHashSet<BytesRef> terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || !(obj instanceof BytesFieldDataFilter)) return false;

            BytesFieldDataFilter that = (BytesFieldDataFilter) obj;
            if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
            if (!terms.equals(that.terms)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            if (hashCode == null) {
                hashCode = fieldData.getFieldNames().indexName().hashCode() + (terms != null ? terms.hashCode() : 0);
            }

            return hashCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("BytesFieldDataFilter:");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "")
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.isEmpty()) return null;

            final BytesValues values = fieldData.load(context).getBytesValues(false); // load fielddata
            return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
                @Override
                protected boolean matchDoc(int doc) {
                    final int numVals = values.setDocument(doc);
                    for (int i = 0; i < numVals; i++) {
                        if (terms.contains(values.nextValue())) {
                            return true;
                        }
                    }

                    return false;
                }
            };
        }
    }

    /**
     * Filters on non-numeric fields.
     */
    protected static class HashedBytesFieldDataFilter extends FieldDataTermsFilter {

        final BytesRefHash terms;
        Integer hashCode;

        protected HashedBytesFieldDataFilter(IndexFieldData fieldData, BytesRefHash terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || !(obj instanceof HashedBytesFieldDataFilter)) return false;

            HashedBytesFieldDataFilter that = (HashedBytesFieldDataFilter) obj;
            if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
            if (terms.size() != that.terms.size()) return false;

            // TODO: best way to do this?
            BytesRef spare = new BytesRef();
            for (long i = 0; i < terms.size(); i++) {
                terms.get(i, spare);
                if (that.terms.find(spare, spare.hashCode()) < 0) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            if (hashCode == null) {
                hashCode = fieldData.getFieldNames().indexName().hashCode();
                if (terms != null) {
                    for (long i = 0; i < terms.size(); i++) {
                        hashCode += terms.code(i);
                    }
                }
            }

            return hashCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("HashedBytesFieldDataFilter:");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "") // TODO: do better?
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.size() == 0) return null;

            final BytesValues values = fieldData.load(context).getBytesValues(true); // load fielddata
            return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
                private final BytesRef spare = new BytesRef();

                @Override
                protected boolean matchDoc(int doc) {

                    final int numVals = values.setDocument(doc);
                    for (int i = 0; i < numVals; i++) {
                        BytesRef term = values.nextValue();
                        if (terms.find(term, values.currentValueHash(), spare) >= 0) {
                            return true;
                        }
                    }

                    return false;

                }
            };
        }
    }

    /**
     * Filters on non-floating point numeric fields.
     */
    protected static class LongsFieldDataFilter extends FieldDataTermsFilter {

        final LongOpenHashSet terms;
        Integer hashCode;

        protected LongsFieldDataFilter(IndexNumericFieldData fieldData, LongOpenHashSet terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || !(obj instanceof LongsFieldDataFilter)) return false;

            LongsFieldDataFilter that = (LongsFieldDataFilter) obj;
            if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
            if (!terms.equals(that.terms)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            if (hashCode == null) {
                hashCode = fieldData.getFieldNames().indexName().hashCode() + (terms != null ? terms.hashCode() : 0);
            }

            return hashCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("LongsFieldDataFilter:");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "")
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.isEmpty()) return null;

            IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
            if (!numericFieldData.getNumericType().isFloatingPoint()) {
                final LongValues values = numericFieldData.load(context).getLongValues(); // load fielddata
                return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
                    @Override
                    protected boolean matchDoc(int doc) {
                        final int numVals = values.setDocument(doc);
                        for (int i = 0; i < numVals; i++) {
                            if (terms.contains(values.nextValue())) {
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
     * Filters on non-floating point numeric fields.
     */
    protected static class HashedLongsFieldDataFilter extends FieldDataTermsFilter {

        final LongHash terms;
        Integer hashCode;

        protected HashedLongsFieldDataFilter(IndexNumericFieldData fieldData, LongHash terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || !(obj instanceof HashedLongsFieldDataFilter)) return false;

            HashedLongsFieldDataFilter that = (HashedLongsFieldDataFilter) obj;
            if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
            if (!terms.equals(that.terms)) return false;
            if (terms.size() != that.terms.size()) return false;

            // TODO: best way to do this?
            for (long i = 0; i < terms.size(); i++) {
                if (that.terms.find(i) < 0) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            if (hashCode == null) {
                hashCode = fieldData.getFieldNames().indexName().hashCode();
                if (terms != null) {
                    for (long i = 0; i < terms.size(); i++) {
                        hashCode += Longs.hashCode(LongHash.hash(terms.get(i)));
                    }
                }
            }

            return hashCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("HashedLongsFieldDataFilter:");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "") // TODO: do better
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.size() == 0) return null;

            IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
            if (!numericFieldData.getNumericType().isFloatingPoint()) {
                final LongValues values = numericFieldData.load(context).getLongValues(); // load fielddata
                return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
                    @Override
                    protected boolean matchDoc(int doc) {
                        final int numVals = values.setDocument(doc);
                        for (int i = 0; i < numVals; i++) {
                            if (terms.find(values.nextValue()) >= 0) {
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
        Integer hashCode;

        protected DoublesFieldDataFilter(IndexNumericFieldData fieldData, DoubleOpenHashSet terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || !(obj instanceof DoublesFieldDataFilter)) return false;

            DoublesFieldDataFilter that = (DoublesFieldDataFilter) obj;
            if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
            if (!terms.equals(that.terms)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            if (hashCode == null) {
                hashCode = fieldData.getFieldNames().indexName().hashCode() + (terms != null ? terms.hashCode() : 0);
            }

            return hashCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("DoublesFieldDataFilter");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "")
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.isEmpty()) return null;

            // verify we have a floating point numeric fielddata
            IndexNumericFieldData indexNumericFieldData = (IndexNumericFieldData) fieldData;
            if (indexNumericFieldData.getNumericType().isFloatingPoint()) {
                final DoubleValues values = indexNumericFieldData.load(context).getDoubleValues(); // load fielddata
                return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
                    @Override
                    protected boolean matchDoc(int doc) {
                        final int numVals = values.setDocument(doc);

                        for (int i = 0; i < numVals; i++) {
                            if (terms.contains(values.nextValue())) {
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
    protected static class HashedDoublesFieldDataFilter extends FieldDataTermsFilter {

        final LongHash terms;
        Integer hashCode;

        protected HashedDoublesFieldDataFilter(IndexNumericFieldData fieldData, LongHash terms) {
            super(fieldData);
            this.terms = terms;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || !(obj instanceof HashedLongsFieldDataFilter)) return false;

            HashedLongsFieldDataFilter that = (HashedLongsFieldDataFilter) obj;
            if (!fieldData.getFieldNames().indexName().equals(that.fieldData.getFieldNames().indexName())) return false;
            if (!terms.equals(that.terms)) return false;
            if (terms.size() != that.terms.size()) return false;

            // TODO: best way to do this?
            for (long i = 0; i < terms.size(); i++) {
                if (that.terms.find(i) < 0) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            if (hashCode == null) {
                hashCode = fieldData.getFieldNames().indexName().hashCode();
                if (terms != null) {
                    for (long i = 0; i < terms.size(); i++) {
                        hashCode += Longs.hashCode(LongHash.hash(terms.get(i)));
                    }
                }
            }

            return hashCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("HashedDoublesFieldDataFilter:");
            return sb
                    .append(fieldData.getFieldNames().indexName())
                    .append(":")
                    .append(terms != null ? terms.toString() : "") // TODO: do better
                    .toString();
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            // make sure there are terms to filter on
            if (terms == null || terms.size() == 0) return null;

            IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
            if (numericFieldData.getNumericType().isFloatingPoint()) {
                final DoubleValues values = numericFieldData.load(context).getDoubleValues(); // load fielddata
                return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
                    @Override
                    protected boolean matchDoc(int doc) {
                        final int numVals = values.setDocument(doc);
                        for (int i = 0; i < numVals; i++) {
                            final double dval = values.nextValue();
                            final long lval = Double.doubleToLongBits(dval);
                            if (terms.find(lval) >= 0) {
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
