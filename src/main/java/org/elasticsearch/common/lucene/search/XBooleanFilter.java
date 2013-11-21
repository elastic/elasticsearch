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
import java.util.*;

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
            if (DocIdSets.isEmpty(set)) {
                return null;
            }
            return set;
        }

        // first, go over and see if we can shortcut the execution
        // and gather Bits if we need to
        List<ResultClause> results = new ArrayList<ResultClause>(clauses.size());
        boolean hasShouldClauses = false;
        boolean hasNonEmptyShouldClause = false;
        boolean hasMustClauses = false;
        boolean hasMustNotClauses = false;
        for (int i = 0; i < clauses.size(); i++) {
            FilterClause clause = clauses.get(i);
            DocIdSet set = clause.getFilter().getDocIdSet(context, acceptDocs);
            if (clause.getOccur() == Occur.MUST) {
                hasMustClauses = true;
                if (DocIdSets.isEmpty(set)) {
                    return null;
                }
            } else if (clause.getOccur() == Occur.SHOULD) {
                hasShouldClauses = true;
                if (DocIdSets.isEmpty(set)) {
                    continue;
                }
                hasNonEmptyShouldClause = true;
            } else if (clause.getOccur() == Occur.MUST_NOT) {
                hasMustNotClauses = true;
                if (DocIdSets.isEmpty(set)) {
                    // we mark empty ones as null for must_not, handle it in the next run...
                    results.add(new ResultClause(null, null, clause));
                    continue;
                }
            }
            Bits bits = null;
            if (!DocIdSets.isFastIterator(set)) {
                bits = set.bits();
            }
            results.add(new ResultClause(set, bits, clause));
        }

        if (hasShouldClauses && !hasNonEmptyShouldClause) {
            return null;
        }

        // now, go over the clauses and apply the "fast" ones first...
        hasNonEmptyShouldClause = false;
        boolean hasBits = false;
        // But first we need to handle the "fast" should clauses, otherwise a should clause can unset docs
        // that don't match with a must or must_not clause.
        List<ResultClause> fastOrClauses = new ArrayList<ResultClause>();
        for (int i = 0; i < results.size(); i++) {
            ResultClause clause = results.get(i);
            // we apply bits in based ones (slow) in the second run
            if (clause.bits != null) {
                hasBits = true;
                continue;
            }
            if (clause.clause.getOccur() == Occur.SHOULD) {
                if (hasMustClauses || hasMustNotClauses) {
                    fastOrClauses.add(clause);
                } else if (res == null) {
                    DocIdSetIterator it = clause.docIdSet.iterator();
                    if (it != null) {
                        hasNonEmptyShouldClause = true;
                        res = new FixedBitSet(reader.maxDoc());
                        res.or(it);
                    }
                } else {
                    DocIdSetIterator it = clause.docIdSet.iterator();
                    if (it != null) {
                        hasNonEmptyShouldClause = true;
                        res.or(it);
                    }
                }
            }
        }

        // Now we safely handle the "fast" must and must_not clauses.
        for (int i = 0; i < results.size(); i++) {
            ResultClause clause = results.get(i);
            // we apply bits in based ones (slow) in the second run
            if (clause.bits != null) {
                hasBits = true;
                continue;
            }
            if (clause.clause.getOccur() == Occur.MUST) {
                DocIdSetIterator it = clause.docIdSet.iterator();
                if (it == null) {
                    return null;
                }
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                    res.or(it);
                } else {
                    res.and(it);
                }
            } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                    res.set(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
                }
                if (clause.docIdSet != null) {
                    DocIdSetIterator it = clause.docIdSet.iterator();
                    if (it != null) {
                        res.andNot(it);
                    }
                }
            }
        }

        if (!hasBits) {
            if (!fastOrClauses.isEmpty()) {
                DocIdSetIterator it = res.iterator();
                at_least_one_should_clause_iter:
                for (int setDoc = it.nextDoc(); setDoc != DocIdSetIterator.NO_MORE_DOCS; setDoc = it.nextDoc()) {
                    for (ResultClause fastOrClause : fastOrClauses) {
                        DocIdSetIterator clauseIterator = fastOrClause.iterator();
                        if (clauseIterator == null) {
                            continue;
                        }
                        if (iteratorMatch(clauseIterator, setDoc)) {
                            hasNonEmptyShouldClause = true;
                            continue at_least_one_should_clause_iter;
                        }
                    }
                    res.clear(setDoc);
                }
            }

            if (hasShouldClauses && !hasNonEmptyShouldClause) {
                return null;
            } else {
                return res;
            }
        }

        // we have some clauses with bits, apply them...
        // we let the "res" drive the computation, and check Bits for that
        List<ResultClause> slowOrClauses = new ArrayList<ResultClause>();
        for (int i = 0; i < results.size(); i++) {
            ResultClause clause = results.get(i);
            if (clause.bits == null) {
                continue;
            }
            if (clause.clause.getOccur() == Occur.SHOULD) {
                if (hasMustClauses || hasMustNotClauses) {
                    slowOrClauses.add(clause);
                } else {
                    if (res == null) {
                        DocIdSetIterator it = clause.docIdSet.iterator();
                        if (it == null) {
                            continue;
                        }
                        hasNonEmptyShouldClause = true;
                        res = new FixedBitSet(reader.maxDoc());
                        res.or(it);
                    } else {
                        for (int doc = 0; doc < reader.maxDoc(); doc++) {
                            if (!res.get(doc) && clause.bits.get(doc)) {
                                hasNonEmptyShouldClause = true;
                                res.set(doc);
                            }
                        }
                    }
                }
            } else if (clause.clause.getOccur() == Occur.MUST) {
                if (res == null) {
                    // nothing we can do, just or it...
                    res = new FixedBitSet(reader.maxDoc());
                    DocIdSetIterator it = clause.docIdSet.iterator();
                    if (it == null) {
                        return null;
                    }
                    res.or(it);
                } else {
                    Bits bits = clause.bits;
                    // use the "res" to drive the iteration
                    DocIdSetIterator it = res.iterator();
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        if (!bits.get(doc)) {
                            res.clear(doc);
                        }
                    }
                }
            } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                if (res == null) {
                    res = new FixedBitSet(reader.maxDoc());
                    res.set(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
                    DocIdSetIterator it = clause.docIdSet.iterator();
                    if (it != null) {
                        res.andNot(it);
                    }
                } else {
                    Bits bits = clause.bits;
                    // let res drive the iteration
                    DocIdSetIterator it = res.iterator();
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        if (bits.get(doc)) {
                            res.clear(doc);
                        }
                    }
                }
            }
        }

        // From a boolean_logic behavior point of view a should clause doesn't have impact on a bool filter if there
        // is already a must or must_not clause. However in the current ES bool filter behaviour at least one should
        // clause must match in order for a doc to be a match. What we do here is checking if matched docs match with
        // any should filter. TODO: Add an option to have disable minimum_should_match=1 behaviour
        if (!slowOrClauses.isEmpty() || !fastOrClauses.isEmpty()) {
            DocIdSetIterator it = res.iterator();
            at_least_one_should_clause_iter:
            for (int setDoc = it.nextDoc(); setDoc != DocIdSetIterator.NO_MORE_DOCS; setDoc = it.nextDoc()) {
                for (ResultClause fastOrClause : fastOrClauses) {
                    DocIdSetIterator clauseIterator = fastOrClause.iterator();
                    if (it == null) {
                        continue;
                    }
                    if (iteratorMatch(clauseIterator, setDoc)) {
                        hasNonEmptyShouldClause = true;
                        continue at_least_one_should_clause_iter;
                    }
                }
                for (ResultClause slowOrClause : slowOrClauses) {
                    if (slowOrClause.bits.get(setDoc)) {
                        hasNonEmptyShouldClause = true;
                        continue at_least_one_should_clause_iter;
                    }
                }
                res.clear(setDoc);
            }
        }

        if (hasShouldClauses && !hasNonEmptyShouldClause) {
            return null;
        } else {
            return res;
        }

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
