/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;

/** Base {@link MappedFieldType} implementation for a field that is indexed
 *  with the inverted index. */
public abstract class TermBasedFieldType extends SimpleMappedFieldType {

    public TermBasedFieldType(
        String name,
        boolean isIndexed,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta
    ) {
        super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
    }

    /** Returns the indexed value used to construct search "values".
     *  This method is used for the default implementations of most
     *  query factory methods such as {@link #termQuery}. */
    protected BytesRef indexedValueForSearch(Object value) {
        return BytesRefs.toBytesRef(value);
    }

    @Override
    public Query termQueryCaseInsensitive(Object value, SearchExecutionContext context) {
        failIfNotIndexed();
        return AutomatonQueries.caseInsensitiveTermQuery(new Term(name(), indexedValueForSearch(value)));
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        failIfNotIndexed();
        return new TermQuery(new Term(name(), indexedValueForSearch(value)));
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        failIfNotIndexed();
        BytesRef[] bytesRefs = values.stream().map(this::indexedValueForSearch).toArray(BytesRef[]::new);
        // O(N) check: if the coordinator pre-sorted the terms in doRewrite, skip the O(N*k) radix sort.
        if (BytesRefs.isSorted(bytesRefs) == false) {
            BytesRefs.radixSort(bytesRefs);
        }
        // SortedSet with null comparator signals natural ordering to TermInSetQuery.packTerms,
        // which skips its internal sort and builds PrefixCodedTerms directly.
        return new TermInSetQuery(name(), new SortedBytesRefSet(bytesRefs));
    }

    // Wraps an already-sorted BytesRef[] as a SortedSet<BytesRef> with a null comparator.
    // SortedSet.comparator()==null signals natural ordering to TermInSetQuery.packTerms,
    // allowing it to skip re-sorting when terms are already in order.
    private static final class SortedBytesRefSet extends AbstractSet<BytesRef> implements SortedSet<BytesRef> {
        private final BytesRef[] sorted;

        SortedBytesRefSet(BytesRef[] sorted) {
            this.sorted = sorted;
        }

        @Override
        public int size() {
            return sorted.length;
        }

        @Override
        public Iterator<BytesRef> iterator() {
            return Arrays.asList(sorted).iterator();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] a) {
            if (a.length < sorted.length) {
                return (T[]) Arrays.copyOf(sorted, sorted.length, a.getClass());
            }
            System.arraycopy(sorted, 0, a, 0, sorted.length);
            if (a.length > sorted.length) {
                a[sorted.length] = null;
            }
            return a;
        }

        @Override
        public Comparator<? super BytesRef> comparator() {
            return null; // null signals natural (BytesRef) ordering to TermInSetQuery
        }

        @Override
        public BytesRef first() {
            if (sorted.length == 0) throw new NoSuchElementException();
            return sorted[0];
        }

        @Override
        public BytesRef last() {
            if (sorted.length == 0) throw new NoSuchElementException();
            return sorted[sorted.length - 1];
        }

        @Override
        public SortedSet<BytesRef> subSet(BytesRef from, BytesRef to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSet<BytesRef> headSet(BytesRef to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSet<BytesRef> tailSet(BytesRef from) {
            throw new UnsupportedOperationException();
        }
    }

}
