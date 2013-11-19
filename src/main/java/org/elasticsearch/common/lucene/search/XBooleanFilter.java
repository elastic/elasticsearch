/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Similar to {@link org.apache.lucene.queries.BooleanFilter}.
 * <p/>
 * But with the following major differences:
 * <ol>
 * <li> The order of the clauses is re-ordered based on rudimentary heuristics in order to execute the bool filter
 * is efficient as possible. The ordering is based on whether a filter is based on {@link FixedBitSet}, the
 * the estimated cost which is fetched from {@link org.apache.lucene.search.DocIdSetIterator#cost()} and whether a
 * clause has a must_not as occur.
 * <li> The minimal should clauses that must match with each doc is configurable.
 * </ol>
 */
public class XBooleanFilter extends Filter implements Iterable<FilterClause> {

    public static XBooleanFilter booleanFilter() {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.setMinimumShouldMatch(1);
        return booleanFilter;
    }

    private final List<FilterClause> clauses = new ArrayList<FilterClause>();
    private int minimumShouldMatch;

    private int numShouldClauses;
    private int numMustNotClauses;
    private int numMustClauses;

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        int size = clauses.size();
        if (size == 0) {
            return null;
        }

        AtomicReader reader = context.reader();
        // shortcut when only have one clause...
        if (size == 1) {
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

        List<ResultClause> resultClauses = new ArrayList<ResultClause>(clauses.size());
        int numEmptyShouldClauses = 0;
        int numEmptyMustNotClauses = 0;
        int numBitsBasedClauses = 0;
        for (FilterClause clause : clauses) {
            Occur occur = clause.getOccur();
            if (occur == Occur.SHOULD && minimumShouldMatch == 0 && (numMustClauses > 0)) {
                continue;
            }

            DocIdSet docIdSet = clause.getFilter().getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(docIdSet)) {
                if (occur == Occur.MUST) {
                    return null;
                } else if (occur == Occur.MUST_NOT) {
                    numEmptyMustNotClauses++;
                    continue;
                } else if (occur == Occur.SHOULD) {
                    numEmptyShouldClauses++;
                    continue;
                }
            }

            DocIdSetIterator iterator = docIdSet.iterator();
            if (iterator == null) {
                if (occur == Occur.MUST) {
                    return null;
                } else if (occur == Occur.MUST_NOT) {
                    numEmptyMustNotClauses++;
                    continue;
                } else if (occur == Occur.SHOULD) {
                    numEmptyShouldClauses++;
                    continue;
                }
            }

            Bits bits = null;
            if (!DocIdSets.isFastIterator(docIdSet)) {
                bits = docIdSet.bits();
                numBitsBasedClauses++;
            }
            resultClauses.add(new ResultClause(bits, docIdSet, clause, iterator));
        }

        // This is consistent with BQ: if msm == 0 and no must and have should and must not, the msm should be 1.
        final int minimumShouldMatch;
        if (this.minimumShouldMatch == 0 && numMustClauses == 0 && numMustNotClauses > 0 && numShouldClauses > 0) {
            minimumShouldMatch = 1;
        } else {
            minimumShouldMatch = this.minimumShouldMatch;
        }

        if (numShouldClauses > 0 && (numShouldClauses - numEmptyShouldClauses) < minimumShouldMatch) {
            return null;
        }

        final ResultClausesProcessor processor;
        if (numShouldClauses == 0) {
            if (numMustNotClauses == 0) {
                if (numBitsBasedClauses == resultClauses.size()) {
                    processor = new SlowOnlyMustClausesProcessor(resultClauses, reader);
                } else {
                    // OnlyMustClausesProcessor seem to be faster if all clauses are slow than using SlowOnlyMustClausesProcessor
                    processor = new OnlyMustClausesProcessor(resultClauses, reader);
                }
            } else {
                processor = new MustAndMustNotClausesProcessor(resultClauses, reader, numEmptyMustNotClauses);
            }
        } else {
            if (numMustClauses == 0 && numMustNotClauses == 0 && minimumShouldMatch <= 1) {
                if (numBitsBasedClauses == resultClauses.size()) {
                    processor = new SlowOnlyShouldClausesProcessor(resultClauses, reader);
                } else {
                    processor = new OnlyShouldClausesProcessor(resultClauses, reader);
                }
            } else {
                if (minimumShouldMatch == 0) {
                    processor = new MustAndMustNotClausesProcessor(resultClauses, reader, numEmptyMustNotClauses);
                } else if (minimumShouldMatch == 1) {
                    processor = new AllClausesProcessor(resultClauses, reader, numEmptyMustNotClauses);
                } else {
                    processor = new MinimumShouldMatchAllClausesProcessor(resultClauses, reader, minimumShouldMatch, numEmptyMustNotClauses);
                }
            }
        }

        return processor.process();
    }

    /**
     * Adds a new FilterClause to the Boolean Filter container
     *
     * @param filterClause A FilterClause object containing a Filter and an Occur parameter
     */
    public void add(FilterClause filterClause) {
        if (filterClause.getOccur() == Occur.MUST) {
            numMustClauses++;
        } else if (filterClause.getOccur() == Occur.MUST_NOT) {
            numMustNotClauses++;
        } else if (filterClause.getOccur() == Occur.SHOULD) {
            numShouldClauses++;
        }
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
     * Sets the minimum should clauses that must match with each document in order for that document to be considered
     * as a match.
     */
    public void setMinimumShouldMatch(int minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    /**
     * Returns an iterator on the clauses in this query. It implements the {@link Iterable} interface to
     * make it possible to do:
     * <pre class="prettyprint">for (FilterClause clause : booleanFilter) {}</pre>
     */
    @Override
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
        if (minimumShouldMatch != other.minimumShouldMatch) {
            return false;
        }
        return clauses.equals(other.clauses);
    }

    @Override
    public int hashCode() {
        return 657153718 ^ clauses.hashCode() + minimumShouldMatch;
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
        buffer.append(')');
        if (minimumShouldMatch > 0) {
            buffer.append('~').append(minimumShouldMatch);
        }
        return buffer.toString();
    }

    static abstract class ResultClausesProcessor {

        private final static Comparator<ResultClause> COMPARATOR = new Comparator<ResultClause>() {
            @Override
            public int compare(ResultClause first, ResultClause second) {
                if (first.bits != null && second.bits == null) {
                    return 1;
                } else if (first.bits == null && second.bits != null) {
                    return -1;
                }

                // TODO: IF FBS we can use cardinality to order clauses. Although #cost() and #cardinality() are unequal in
                // FBS, it is just about re-ordering the clauses. However for this we need to somehow cache FBS#cardinality()

                // must_not clause with higher cost is of more interest, b/c the set bits get inversed.
                boolean revert = first.clause.getOccur() != Occur.MUST_NOT && second.clause.getOccur() == Occur.MUST_NOT;
                long cmpCost = first.iterator.cost() - second.iterator.cost();
                if (cmpCost < 0) {
                    return revert ? 1 : -1;
                } else if (cmpCost > 0) {
                    return revert ? -1 : 1;
                } else {
                    return 0;
                }
            }
        };

        protected final List<ResultClause> resultClauses;
        protected final AtomicReader reader;

        protected final int minimumShouldMatch;
        protected final int numEmptyMustNotClauses;

        protected ResultClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader,
                                         int minimumShouldMatch, int numEmptyMustNotClauses) {
            this.resultClauses = resultClauses;
            this.reader = reader;
            this.minimumShouldMatch = minimumShouldMatch;
            this.numEmptyMustNotClauses = numEmptyMustNotClauses;
        }

        public DocIdSet process() throws IOException {
            CollectionUtil.timSort(resultClauses, comparator());
            return doProcess();
        }

        protected abstract DocIdSet doProcess() throws IOException;

        protected Comparator<ResultClause> comparator() {
            return COMPARATOR;
        }

    }

    final static class MinimumShouldMatchAllClausesProcessor extends ResultClausesProcessor {

        private MinimumShouldMatchAllClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader,
                                                      int minimumShouldMatch, int numEmptyMustNotClauses) {
            super(resultClauses, reader, minimumShouldMatch, numEmptyMustNotClauses);
        }

        @Override
        public DocIdSet doProcess() throws IOException {
            ResultBuilder builder = new ResultBuilder(reader, numEmptyMustNotClauses);
            List<ResultClause> shouldClauses = new ArrayList<ResultClause>();

            int i = 0;
            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);
                if (clause.bits != null) {
                    break;
                }

                if (clause.clause.getOccur() == Occur.SHOULD) {
                    shouldClauses.add(clause);
                    continue;
                }

                if (clause.clause.getOccur() == Occur.MUST) {
                    builder.must(clause.iterator);
                } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                    builder.mustNot(clause.iterator);
                }
            }

            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);

                if (clause.clause.getOccur() == Occur.SHOULD) {
                    shouldClauses.add(clause);
                    continue;
                }

                if (clause.clause.getOccur() == Occur.MUST) {
                    builder.must(clause.iterator, clause.bits);
                } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                    builder.mustNot(clause.iterator, clause.bits);
                }
            }

            CollectionUtil.timSort(shouldClauses, comparator());
            builder.shouldWithMinimumShouldMatch(shouldClauses, minimumShouldMatch);

            return builder.build();
        }

    }

    final static class AllClausesProcessor extends ResultClausesProcessor {

        AllClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader, int numEmptyMustNotClauses) {
            super(resultClauses, reader, -1, numEmptyMustNotClauses);
        }

        @Override
        public DocIdSet doProcess() throws IOException {
            ResultBuilder builder = new ResultBuilder(reader, numEmptyMustNotClauses);
            List<ResultClause> shouldClauses = new ArrayList<ResultClause>();

            int i = 0;
            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);
                if (clause.bits != null) {
                    break;
                }

                if (clause.clause.getOccur() == Occur.SHOULD) {
                    shouldClauses.add(clause);
                    continue;
                }

                if (clause.clause.getOccur() == Occur.MUST) {
                    builder.must(clause.iterator);
                } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                    builder.mustNot(clause.iterator);
                }
            }

            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);

                if (clause.clause.getOccur() == Occur.SHOULD) {
                    shouldClauses.add(clause);
                    continue;
                }

                if (clause.clause.getOccur() == Occur.MUST) {
                    builder.must(clause.iterator, clause.bits);
                } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                    builder.mustNot(clause.iterator, clause.bits);
                }
            }

            if (!shouldClauses.isEmpty()) {
                CollectionUtil.timSort(shouldClauses, comparator());
                builder.should(shouldClauses);
            }
            return builder.build();
        }

    }

    private final static class MustAndMustNotClausesProcessor extends ResultClausesProcessor {

        private MustAndMustNotClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader, int numEmptyMustNotClauses) {
            super(resultClauses, reader, -1, numEmptyMustNotClauses);
        }

        @Override
        public DocIdSet doProcess() throws IOException {
            ResultBuilder builder = new ResultBuilder(reader, numEmptyMustNotClauses);

            int i = 0;
            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);
                if (clause.bits != null) {
                    break;
                }

                if (clause.clause.getOccur() == Occur.MUST) {
                    builder.must(clause.iterator);
                } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                    builder.mustNot(clause.iterator);
                }
            }

            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);
                if (clause.clause.getOccur() == Occur.MUST) {
                    builder.must(clause.iterator, clause.bits);
                } else if (clause.clause.getOccur() == Occur.MUST_NOT) {
                    builder.mustNot(clause.iterator, clause.bits);
                }
            }

            return builder.build();
        }
    }

    // TODO: potentially when all clauses are FBS, we can only pick the clause with lowest cardinality and omit the
    // other clauses.
    private final static class OnlyShouldClausesProcessor extends ResultClausesProcessor {

        private OnlyShouldClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader) {
            super(resultClauses, reader, -1, -1);
        }

        @Override
        public DocIdSet doProcess() throws IOException {
            ResultBuilder builder = new ResultBuilder(reader, 0);

            int i = 0;
            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);
                if (clause.bits != null) {
                    break;
                }
                builder.should(clause.iterator);
            }

            if (i < resultClauses.size()) {
                builder.shouldSlowClausesOnly(resultClauses.subList(i, resultClauses.size()));
            }
            return builder.build();
        }
    }

    final static class SlowOnlyShouldClausesProcessor extends ResultClausesProcessor {

        private SlowOnlyShouldClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader) {
            super(resultClauses, reader, -1, -1);
        }

        @Override
        public DocIdSet doProcess() throws IOException {
            DocIdSet[] docIdSets = new DocIdSet[resultClauses.size()];
            for (int i = 0; i < docIdSets.length; i++) {
                docIdSets[i] = resultClauses.get(i).docIdSet;
            }
            return new OrDocIdSet(docIdSets);
        }
    }

    final static class OnlyMustClausesProcessor extends ResultClausesProcessor {

        private OnlyMustClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader) {
            super(resultClauses, reader, -1, -1);
        }

        @Override
        public DocIdSet doProcess() throws IOException {
            ResultBuilder builder = new ResultBuilder(reader, 0);

            int i = 0;
            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);
                if (clause.bits != null) {
                    break;
                }
                builder.must(clause.iterator);
            }

            for (; i < resultClauses.size(); i++) {
                ResultClause clause = resultClauses.get(i);
                builder.must(clause.iterator, clause.bits);
            }

            return builder.build();
        }
    }

    final static class SlowOnlyMustClausesProcessor extends ResultClausesProcessor {

        private SlowOnlyMustClausesProcessor(List<ResultClause> resultClauses, AtomicReader reader) {
            super(resultClauses, reader, -1, -1);
        }

        @Override
        public DocIdSet doProcess() throws IOException {
            DocIdSet[] docIdSets = new DocIdSet[resultClauses.size()];
            for (int i = 0; i < docIdSets.length; i++) {
                docIdSets[i] = resultClauses.get(i).docIdSet;
            }
            return new AndDocIdSet(docIdSets);
        }
    }

    final static class ResultClause {

        final Bits bits;
        final DocIdSet docIdSet;
        final FilterClause clause;
        final DocIdSetIterator iterator;

        ResultClause(Bits bits, DocIdSet docIdSet, FilterClause clause, DocIdSetIterator iterator) {
            this.bits = bits;
            this.docIdSet = docIdSet;
            this.clause = clause;
            this.iterator = iterator;
        }

    }

    final static class ResultBuilder {

        private final AtomicReader reader;
        private FixedBitSet result;

        ResultBuilder(AtomicReader reader, int numEmptyMustNotClauses) {
            this.reader = reader;
            if (numEmptyMustNotClauses > 0) {
                result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc());
            }
        }

        void must(DocIdSetIterator iterator) throws IOException {
            if (result == null) {
                result = new FixedBitSet(reader.maxDoc());
                result.or(iterator);
            } else {
                result.and(iterator);
            }
        }

        void must(DocIdSetIterator iterator, Bits bits) throws IOException {
            if (result == null) {
                result = new FixedBitSet(reader.maxDoc());
                result.or(iterator);
            } else {
                // use the "res" to drive the iteration
                DocIdSetIterator it = result.iterator();
                for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    if (!bits.get(doc)) {
                        result.clear(doc);
                    }
                }
            }
        }

        void mustNot(DocIdSetIterator iterator) throws IOException {
            if (result == null) {
                result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc());
            }
            result.andNot(iterator);
        }

        void mustNot(DocIdSetIterator iterator, Bits bits) throws IOException {
            if (result == null) {
                result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc());
                result.andNot(iterator);
            } else {
                // let res drive the iteration
                DocIdSetIterator it = result.iterator();
                for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    if (bits.get(doc)) {
                        result.clear(doc);
                    }
                }
            }
        }

        void should(DocIdSetIterator iterator) throws IOException {
            if (result == null) {
                result = new FixedBitSet(reader.maxDoc());
            }
            result.or(iterator);
        }

        void shouldSlowClausesOnly(List<ResultClause> shouldClauses) throws IOException {
            if (result == null) {
                result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc());
            }

            int nextSetDoc;
            DocIdSetIterator it = result.iterator();
            for (int prevSetDoc = -1; prevSetDoc != DocIdSetIterator.NO_MORE_DOCS; prevSetDoc = nextSetDoc) {
                nextSetDoc = it.nextDoc();
                int end = nextSetDoc;
                if (nextSetDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    end = reader.maxDoc();
                }

                outer: for (int doc = prevSetDoc + 1; doc < end; doc++) {
                    for (ResultClause shouldClause : shouldClauses) {
                        if (shouldClause.bits.get(doc)) {
                            result.set(doc);
                            continue outer;
                        }
                    }
                }
            }
        }

        // Iterates over all matching docs once and unsets matching docs if not matching with any should clauses.
        void should(List<ResultClause> shouldClauses) throws IOException {
            if (result == null) {
                result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc());
            }

            DocIdSetIterator resultIterator = result.iterator();
            outer:
            for (int setDoc = resultIterator.nextDoc(); setDoc != DocIdSetIterator.NO_MORE_DOCS; setDoc = resultIterator.nextDoc()) {
                for (ResultClause shouldClause : shouldClauses) {
                    if (shouldClause.bits != null) {
                        if (shouldClause.bits.get(setDoc)) {
                            continue outer;
                        }
                    } else {
                        int current = shouldClause.iterator.docID();
                        if (current == DocIdSetIterator.NO_MORE_DOCS) {
                            continue; // maybe remove these clauses from execution?
                        }
                        if (current == setDoc) {
                            continue outer;
                        } else if (setDoc > current && shouldClause.iterator.advance(setDoc) == setDoc) {
                            continue outer;
                        }
                    }
                }
                result.clear(setDoc);
            }
        }

        void shouldWithMinimumShouldMatch(List<ResultClause> shouldClauses, int minimumShouldMatch) throws IOException {
            int initialMatchedShouldClauses = 0;
            if (result == null) {

                result = new FixedBitSet(reader.maxDoc());
                if (shouldClauses.size() == minimumShouldMatch) {
                    // at least one of the result clauses needs to match, so we pick the first one and let that one drive the result.
                    Iterator<ResultClause> iterator = shouldClauses.iterator();
                    int numAllowedNoResultClauses = shouldClauses.size() - minimumShouldMatch;
                    while (true){
                        result.or(iterator.next().iterator);
                        iterator.remove();
                        if (result.cardinality() > 0) {
                            break;
                        }
                        if (--numAllowedNoResultClauses < 0) {
                            return;
                        }
                    }

                    // Nothing has matched before, so we safely assume that each matching doc has already
                    // a should match clause count of 1.
                    initialMatchedShouldClauses = 1;
                } else {
                    // Bummer we have to check all the docs in this segment...
                    result.set(0, reader.maxDoc());
                }
            }

            DocIdSetIterator resultIterator = result.iterator();
            outer:
            for (int setDoc = resultIterator.nextDoc(); setDoc != DocIdSetIterator.NO_MORE_DOCS; setDoc = resultIterator.nextDoc()) {
                int matchingShouldClauses = initialMatchedShouldClauses;
                for (ResultClause shouldClause : shouldClauses) {
                    if (shouldClause.bits != null) {
                        if (shouldClause.bits.get(setDoc)) {
                            matchingShouldClauses++;
                        }
                    } else {
                        int current = shouldClause.iterator.docID();
                        if (current == DocIdSetIterator.NO_MORE_DOCS) {
                            continue; // maybe remove these clauses from execution?
                        }
                        if (current == setDoc) {
                            matchingShouldClauses++;
                        } else if (setDoc > current && shouldClause.iterator.advance(setDoc) == setDoc) {
                            matchingShouldClauses++;
                        }
                    }
                    if (matchingShouldClauses >= minimumShouldMatch) {
                        continue outer;
                    }
                }

                if (matchingShouldClauses < minimumShouldMatch) {
                    result.clear(setDoc);
                }
            }
        }

        FixedBitSet build() {
            if (result == null) {
                return null;
            } else {
                return result.cardinality() == 0 ? null : result;
            }
        }
    }

}
