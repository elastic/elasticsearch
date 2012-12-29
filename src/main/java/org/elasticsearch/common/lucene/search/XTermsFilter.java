/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.*;

/**
 * Constructs a filter for docs matching any of the terms added to this class.
 * Unlike a RangeFilter this can be used for filtering on multiple terms that are not necessarily in
 * a sequence. An example might be a collection of primary keys from a database query result or perhaps
 * a choice of "category" labels picked by the end user. As a filter, this is much faster than the
 * equivalent query (a BooleanQuery with many "should" TermQueries)
 */
// LUCENE 4.1 UPGRADE: Just use TermsFilter once upgrading to 4.1, its a copy
public final class XTermsFilter extends Filter {

    /*
     * this class is often used for large number of terms in a single field.
     * to optimize for this case and to be filter-cache friendly we
     * serialize all terms into a single byte array and store offsets
     * in a parallel array to keep the # of object constant and speed up
     * equals / hashcode.
     *
     * This adds quite a bit of complexity but allows large term filters to
     * be efficient for GC and cache-lookups
     */
    private final int[] offsets;
    private final byte[] termsBytes;
    private final TermsAndField[] termsAndFields;
    private final int hashCode; // cached hashcode for fast cache lookups
    private static final int PRIME = 31;

    /**
     * Creates a new {@link XTermsFilter} from the given list. The list
     * can contain duplicate terms and multiple fields.
     */
    public XTermsFilter(final List<Term> terms) {
        this(new FieldAndTermEnum() {
            // we need to sort for deduplication and to have a common cache key
            final Iterator<Term> iter = sort(terms).iterator();

            @Override
            public BytesRef next() {
                if (iter.hasNext()) {
                    Term next = iter.next();
                    field = next.field();
                    return next.bytes();
                }
                return null;
            }
        }, terms.size());
    }

    /**
     * Creates a new {@link XTermsFilter} from the given {@link BytesRef} list for
     * a single field.
     */
    public XTermsFilter(final String field, final List<BytesRef> terms) {
        this(new FieldAndTermEnum(field) {
            // we need to sort for deduplication and to have a common cache key
            final Iterator<BytesRef> iter = sort(terms).iterator();

            @Override
            public BytesRef next() {
                if (iter.hasNext()) {
                    return iter.next();
                }
                return null;
            }
        }, terms.size());
    }

    /**
     * Creates a new {@link XTermsFilter} from the given {@link BytesRef} array for
     * a single field.
     */
    public XTermsFilter(final String field, final BytesRef... terms) {
        // this ctor prevents unnecessary Term creations
        this(field, Arrays.asList(terms));
    }

    /**
     * Creates a new {@link XTermsFilter} from the given array. The array can
     * contain duplicate terms and multiple fields.
     */
    public XTermsFilter(final Term... terms) {
        this(Arrays.asList(terms));
    }


    private XTermsFilter(FieldAndTermEnum iter, int length) {
        // TODO: maybe use oal.index.PrefixCodedTerms instead?
        // If number of terms is more than a few hundred it
        // should be a win

        // TODO: we also pack terms in FieldCache/DocValues
        // ... maybe we can refactor to share that code

        // TODO: yet another option is to build the union of the terms in
        // an automaton an call intersect on the termsenum if the density is high

        int hash = 9;
        byte[] serializedTerms = new byte[0];
        this.offsets = new int[length + 1];
        int lastEndOffset = 0;
        int index = 0;
        ArrayList<TermsAndField> termsAndFields = new ArrayList<TermsAndField>();
        TermsAndField lastTermsAndField = null;
        BytesRef previousTerm = null;
        String previousField = null;
        BytesRef currentTerm;
        String currentField;
        while ((currentTerm = iter.next()) != null) {
            currentField = iter.field();
            if (currentField == null) {
                throw new IllegalArgumentException("Field must not be null");
            }
            if (previousField != null) {
                // deduplicate
                if (previousField.equals(currentField)) {
                    if (previousTerm.bytesEquals(currentTerm)) {
                        continue;
                    }
                } else {
                    final int start = lastTermsAndField == null ? 0 : lastTermsAndField.end;
                    lastTermsAndField = new TermsAndField(start, index, previousField);
                    termsAndFields.add(lastTermsAndField);
                }
            }
            hash = PRIME * hash + currentField.hashCode();
            hash = PRIME * hash + currentTerm.hashCode();
            if (serializedTerms.length < lastEndOffset + currentTerm.length) {
                serializedTerms = ArrayUtil.grow(serializedTerms, lastEndOffset + currentTerm.length);
            }
            System.arraycopy(currentTerm.bytes, currentTerm.offset, serializedTerms, lastEndOffset, currentTerm.length);
            offsets[index] = lastEndOffset;
            lastEndOffset += currentTerm.length;
            index++;
            previousTerm = currentTerm;
            previousField = currentField;
        }
        offsets[index] = lastEndOffset;
        final int start = lastTermsAndField == null ? 0 : lastTermsAndField.end;
        lastTermsAndField = new TermsAndField(start, index, previousField);
        termsAndFields.add(lastTermsAndField);
        this.termsBytes = ArrayUtil.shrink(serializedTerms, lastEndOffset);
        this.termsAndFields = termsAndFields.toArray(new TermsAndField[termsAndFields.size()]);
        this.hashCode = hash;

    }


    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        final AtomicReader reader = context.reader();
        FixedBitSet result = null;  // lazy init if needed - no need to create a big bitset ahead of time
        final Fields fields = reader.fields();
        final BytesRef spare = new BytesRef(this.termsBytes);
        if (fields == null) {
            return result;
        }
        Terms terms = null;
        TermsEnum termsEnum = null;
        DocsEnum docs = null;
        for (TermsAndField termsAndField : this.termsAndFields) {
            if ((terms = fields.terms(termsAndField.field)) != null) {
                termsEnum = terms.iterator(termsEnum); // this won't return null
                for (int i = termsAndField.start; i < termsAndField.end; i++) {
                    spare.offset = offsets[i];
                    spare.length = offsets[i + 1] - offsets[i];
                    if (termsEnum.seekExact(spare, false)) { // don't use cache since we could pollute the cache here easily
                        docs = termsEnum.docs(acceptDocs, docs, 0); // no freq since we don't need them
                        if (result == null) {
                            if (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                result = new FixedBitSet(reader.maxDoc());
                                // lazy init but don't do it in the hot loop since we could read many docs
                                result.set(docs.docID());
                            }
                        }
                        while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            result.set(docs.docID());
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != this.getClass())) {
            return false;
        }

        XTermsFilter test = (XTermsFilter) obj;
        if (test.hashCode == hashCode && this.termsAndFields.length == test.termsAndFields.length) {
            // first check the fields before even comparing the bytes
            for (int i = 0; i < termsAndFields.length; i++) {
                TermsAndField current = termsAndFields[i];
                if (!current.equals(test.termsAndFields[i])) {
                    return false;
                }
            }
            // straight byte comparison since we sort they must be identical
            int end = offsets[termsAndFields.length];
            byte[] left = this.termsBytes;
            byte[] right = test.termsBytes;
            for (int i = 0; i < end; i++) {
                if (left[i] != right[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        BytesRef spare = new BytesRef(termsBytes);
        boolean first = true;
        for (int i = 0; i < termsAndFields.length; i++) {
            TermsAndField current = termsAndFields[i];
            for (int j = current.start; j < current.end; j++) {
                spare.offset = offsets[j];
                spare.length = offsets[j + 1] - offsets[j];
                if (!first) {
                    builder.append(' ');
                }
                first = false;
                builder.append(current.field).append(':');
                builder.append(spare.utf8ToString());
            }
        }

        return builder.toString();
    }

    private static final class TermsAndField {
        final int start;
        final int end;
        final String field;


        TermsAndField(int start, int end, String field) {
            super();
            this.start = start;
            this.end = end;
            this.field = field;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((field == null) ? 0 : field.hashCode());
            result = prime * result + end;
            result = prime * result + start;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            TermsAndField other = (TermsAndField) obj;
            if (field == null) {
                if (other.field != null) return false;
            } else if (!field.equals(other.field)) return false;
            if (end != other.end) return false;
            if (start != other.start) return false;
            return true;
        }

    }

    private static abstract class FieldAndTermEnum {
        protected String field;

        public abstract BytesRef next();

        public FieldAndTermEnum() {
        }

        public FieldAndTermEnum(String field) {
            this.field = field;
        }

        public String field() {
            return field;
        }
    }

    /*
     * simple utility that returns the in-place sorted list
     */
    private static <T extends Comparable<? super T>> List<T> sort(List<T> toSort) {
        if (toSort.isEmpty()) {
            throw new IllegalArgumentException("no terms provided");
        }
        Collections.sort(toSort);
        return toSort;
    }
}
