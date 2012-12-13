package org.elasticsearch.common.lucene.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.AllDocIdSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.docset.NotDocIdSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Similar to {@link org.apache.lucene.queries.BooleanFilter}.
 * <p/>
 * Our own variance mainly differs by the fact that we pass the acceptDocs down to the filters
 * and don't filter based on them at the end. Our logic is a bit different, and we filter based on that
 * at the top level filter chain.
 */
public class XBooleanFilter extends Filter implements Iterable<FilterClause> {

    final List<FilterClause> clauses = new ArrayList<FilterClause>();

    /**
     * Returns the a DocIdSetIterator representing the Boolean composition
     * of the filters that have been added.
     */
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        FixedBitSet res = null;
        final AtomicReader reader = context.reader();

        // optimize single case...
        if (clauses.size() == 1) {
            FilterClause clause = clauses.get(0);
            DocIdSet set = clause.getFilter().getDocIdSet(context, acceptDocs);
            if (clause.getOccur() == Occur.MUST_NOT) {
                if (DocIdSets.isEmpty(set)) {
                    return new AllDocIdSet(reader.maxDoc());
                } else {
                    return new NotDocIdSet(set, reader.maxDoc());
                }
            }
            // SHOULD or MUST, just return the set...
            return set;
        }

        boolean hasShouldClauses = false;
        for (final FilterClause fc : clauses) {
            if (fc.getOccur() == Occur.SHOULD) {
                hasShouldClauses = true;
                final DocIdSetIterator disi = getDISI(fc.getFilter(), context, acceptDocs);
                if (disi == null) continue;
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                }
                res.or(disi);
            }
        }
        if (hasShouldClauses && res == null)
            return null;

        for (final FilterClause fc : clauses) {
            if (fc.getOccur() == Occur.MUST_NOT) {
                if (res == null) {
                    assert !hasShouldClauses;
                    res = new FixedBitSet(reader.maxDoc());
                    res.set(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
                }
                final DocIdSetIterator disi = getDISI(fc.getFilter(), context, acceptDocs);
                if (disi != null) {
                    res.andNot(disi);
                }
            }
        }

        for (final FilterClause fc : clauses) {
            if (fc.getOccur() == Occur.MUST) {
                final DocIdSetIterator disi = getDISI(fc.getFilter(), context, acceptDocs);
                if (disi == null) {
                    return null;
                }
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                    res.or(disi);
                } else {
                    res.and(disi);
                }
            }
        }

        // don't wrap, based on our own strategy of doing the wrapping on the filtered query level
        //return res != null ? BitsFilteredDocIdSet.wrap(res, acceptDocs) : DocIdSet.EMPTY_DOCIDSET;
        return res;
    }

    private static DocIdSetIterator getDISI(Filter filter, AtomicReaderContext context, Bits acceptDocs)
            throws IOException {
        final DocIdSet set = filter.getDocIdSet(context, acceptDocs);
        return (set == null || set == DocIdSet.EMPTY_DOCIDSET) ? null : set.iterator();
    }

    /**
     * Adds a new FilterClause to the Boolean Filter container
     *
     * @param filterClause A FilterClause object containing a Filter and an Occur parameter
     */
    public void add(FilterClause filterClause) {
        clauses.add(filterClause);
    }

    public final void add(Filter filter, Occur occur) {
        add(new FilterClause(filter, occur));
    }

    /**
     * Returns the list of clauses
     */
    public List<FilterClause> clauses() {
        return clauses;
    }

    /**
     * Returns an iterator on the clauses in this query. It implements the {@link Iterable} interface to
     * make it possible to do:
     * <pre class="prettyprint">for (FilterClause clause : booleanFilter) {}</pre>
     */
    public final Iterator<FilterClause> iterator() {
        return clauses().iterator();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (obj.getClass() != this.getClass())) {
            return false;
        }

        final XBooleanFilter other = (XBooleanFilter) obj;
        return clauses.equals(other.clauses);
    }

    @Override
    public int hashCode() {
        return 657153718 ^ clauses.hashCode();
    }

    /**
     * Prints a user-readable version of this Filter.
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder("BooleanFilter(");
        final int minLen = buffer.length();
        for (final FilterClause c : clauses) {
            if (buffer.length() > minLen) {
                buffer.append(' ');
            }
            buffer.append(c);
        }
        return buffer.append(')').toString();
    }
}
