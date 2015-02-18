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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.lucene.docset.AllDocIdSet;
import org.elasticsearch.common.lucene.docset.AndDocIdSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.docset.NotDocIdSet;
import org.elasticsearch.common.lucene.docset.OrDocIdSet.OrBits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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

    private static final Comparator<DocIdSetIterator> COST_DESCENDING = new Comparator<DocIdSetIterator>() {
        @Override
        public int compare(DocIdSetIterator o1, DocIdSetIterator o2) {
            return Long.compare(o2.cost(), o1.cost());
        }
    };
    private static final Comparator<DocIdSetIterator> COST_ASCENDING = new Comparator<DocIdSetIterator>() {
        @Override
        public int compare(DocIdSetIterator o1, DocIdSetIterator o2) {
            return Long.compare(o1.cost(), o2.cost());
        }
    };

    final List<FilterClause> clauses = new ArrayList<>();

    /**
     * Returns the a DocIdSetIterator representing the Boolean composition
     * of the filters that have been added.
     */
    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final int maxDoc = context.reader().maxDoc();

        // the 0-clauses case is ambiguous because an empty OR filter should return nothing
        // while an empty AND filter should return all docs, so we handle this case explicitely
        if (clauses.isEmpty()) {
            return null;
        }

        // optimize single case...
        if (clauses.size() == 1) {
            FilterClause clause = clauses.get(0);
            DocIdSet set = clause.getFilter().getDocIdSet(context, acceptDocs);
            if (clause.getOccur() == Occur.MUST_NOT) {
                if (DocIdSets.isEmpty(set)) {
                    return new AllDocIdSet(maxDoc);
                } else {
                    return new NotDocIdSet(set, maxDoc);
                }
            }
            // SHOULD or MUST, just return the set...
            if (DocIdSets.isEmpty(set)) {
                return null;
            }
            return set;
        }

        // We have several clauses, try to organize things to make it easier to process
        List<DocIdSetIterator> shouldIterators = new ArrayList<>();
        List<Bits> shouldBits = new ArrayList<>();
        boolean hasShouldClauses = false;

        List<DocIdSetIterator> requiredIterators = new ArrayList<>();
        List<DocIdSetIterator> excludedIterators = new ArrayList<>();

        List<Bits> requiredBits = new ArrayList<>();
        List<Bits> excludedBits = new ArrayList<>();

        for (FilterClause clause : clauses) {
            DocIdSet set = clause.getFilter().getDocIdSet(context, null);
            DocIdSetIterator it = null;
            Bits bits = null;
            if (DocIdSets.isEmpty(set) == false) {
                it = set.iterator();
                if (it != null) {
                    bits = set.bits();
                }
            }

            switch (clause.getOccur()) {
            case SHOULD:
                hasShouldClauses = true;
                if (it == null) {
                    // continue, but we recorded that there is at least one should clause
                    // so that if all iterators are null we know that nothing matches this
                    // filter since at least one SHOULD clause needs to match
                } else if (bits != null && DocIdSets.isBroken(it)) {
                    shouldBits.add(bits);
                } else {
                    shouldIterators.add(it);
                }
                break;
            case MUST:
                if (it == null) {
                    // no documents matched a clause that is compulsory, then nothing matches at all
                    return null;
                } else if (bits != null && DocIdSets.isBroken(it)) {
                    requiredBits.add(bits);
                } else {
                    requiredIterators.add(it);
                }
                break;
            case MUST_NOT:
                if (it == null) {
                    // ignore
                } else if (bits != null && DocIdSets.isBroken(it)) {
                    excludedBits.add(bits);
                } else {
                    excludedIterators.add(it);
                }
                break;
            default:
                throw new AssertionError();
            }
        }

        // Since BooleanFilter requires that at least one SHOULD clause matches,
        // transform the SHOULD clauses into a MUST clause

        if (hasShouldClauses) {
            if (shouldIterators.isEmpty() && shouldBits.isEmpty()) {
                // we had should clauses, but they all produced empty sets
                // yet BooleanFilter requires that at least one clause matches
                // so it means we do not match anything
                return null;
            } else if (shouldIterators.size() == 1 && shouldBits.isEmpty()) {
                requiredIterators.add(shouldIterators.get(0));
            } else {
                // apply high-cardinality should clauses first
                CollectionUtil.timSort(shouldIterators, COST_DESCENDING);

                BitDocIdSet.Builder shouldBuilder = null;
                for (DocIdSetIterator it : shouldIterators) {
                    if (shouldBuilder == null) {
                        shouldBuilder = new BitDocIdSet.Builder(maxDoc);
                    }
                    shouldBuilder.or(it);
                }

                if (shouldBuilder != null && shouldBits.isEmpty() == false) {
                    // we have both iterators and bits, there is no way to compute
                    // the union efficiently, so we just transform the iterators into
                    // bits
                    // add first since these are fast bits
                    shouldBits.add(0, shouldBuilder.build().bits());
                    shouldBuilder = null;
                }

                if (shouldBuilder == null) {
                    // only bits
                    assert shouldBits.size() >= 1;
                    if (shouldBits.size() == 1) {
                        requiredBits.add(shouldBits.get(0));
                    } else {
                        requiredBits.add(new OrBits(shouldBits.toArray(new Bits[shouldBits.size()])));
                    }
                } else {
                    assert shouldBits.isEmpty();
                    // only iterators, we can add the merged iterator to the list of required iterators
                    requiredIterators.add(shouldBuilder.build().iterator());
                }
            }
        } else {
            assert shouldIterators.isEmpty();
            assert shouldBits.isEmpty();
        }

        // From now on, we don't have to care about SHOULD clauses anymore since we upgraded
        // them to required clauses (if necessary)

        // cheap iterators first to make intersection faster
        CollectionUtil.timSort(requiredIterators, COST_ASCENDING);
        CollectionUtil.timSort(excludedIterators, COST_ASCENDING);

        // Intersect iterators
        BitDocIdSet.Builder res = null;
        for (DocIdSetIterator iterator : requiredIterators) {
            if (res == null) {
                res = new BitDocIdSet.Builder(maxDoc);
                res.or(iterator);
            } else {
                res.and(iterator);
            }
        }
        for (DocIdSetIterator iterator : excludedIterators) {
            if (res == null) {
                res = new BitDocIdSet.Builder(maxDoc, true);
            }
            res.andNot(iterator);
        }

        // Transform the excluded bits into required bits
        if (excludedBits.isEmpty() == false) {
            Bits excluded;
            if (excludedBits.size() == 1) {
                excluded = excludedBits.get(0);
            } else {
                excluded = new OrBits(excludedBits.toArray(new Bits[excludedBits.size()]));
            }
            requiredBits.add(new NotDocIdSet.NotBits(excluded));
        }

        // The only thing left to do is to intersect 'res' with 'requiredBits'

        // the main doc id set that will drive iteration
        DocIdSet main;
        if (res == null) {
            main = new AllDocIdSet(maxDoc);
        } else {
            main = res.build();
        }

        // apply accepted docs and compute the bits to filter with
        // accepted docs are added first since they are fast and will help not computing anything on deleted docs
        if (acceptDocs != null) {
            requiredBits.add(0, acceptDocs);
        }
        // the random-access filter that we will apply to 'main'
        Bits filter;
        if (requiredBits.isEmpty()) {
            filter = null;
        } else if (requiredBits.size() == 1) {
            filter = requiredBits.get(0);
        } else {
            filter = new AndDocIdSet.AndBits(requiredBits.toArray(new Bits[requiredBits.size()]));
        }

        return BitsFilteredDocIdSet.wrap(main, filter);
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
    public String toString(String field) {
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

    static class ResultClause {

        public final DocIdSet docIdSet;
        public final Bits bits;
        public final FilterClause clause;

        DocIdSetIterator docIdSetIterator;

        ResultClause(DocIdSet docIdSet, Bits bits, FilterClause clause) {
            this.docIdSet = docIdSet;
            this.bits = bits;
            this.clause = clause;
        }

        /**
         * @return An iterator, but caches it for subsequent usage. Don't use if iterator is consumed in one invocation.
         */
        DocIdSetIterator iterator() throws IOException {
            if (docIdSetIterator != null) {
                return docIdSetIterator;
            } else {
                return docIdSetIterator = docIdSet.iterator();
            }
        }

    }

    static boolean iteratorMatch(DocIdSetIterator docIdSetIterator, int target) throws IOException {
        assert docIdSetIterator != null;
        int current = docIdSetIterator.docID();
        if (current == DocIdSetIterator.NO_MORE_DOCS || target < current) {
            return false;
        } else {
            if (current == target) {
                return true;
            } else {
                return docIdSetIterator.advance(target) == target;
            }
        }
    }

}
