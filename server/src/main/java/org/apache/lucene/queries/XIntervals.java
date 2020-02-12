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

package org.apache.lucene.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.IntervalMatchesIterator;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Replacement for {@link Intervals#wildcard(BytesRef)} and {@link Intervals#prefix(BytesRef)}
 * until LUCENE-9050 is merged
 */
public final class XIntervals {

    private XIntervals() {}

    public static IntervalsSource wildcard(BytesRef wildcard) {
        CompiledAutomaton ca = new CompiledAutomaton(WildcardQuery.toAutomaton(new Term("", wildcard)));
        return new MultiTermIntervalsSource(ca, 128, wildcard.utf8ToString());
    }

    public static IntervalsSource prefix(BytesRef prefix) {
        CompiledAutomaton ca = new CompiledAutomaton(PrefixQuery.toAutomaton(prefix));
        return new MultiTermIntervalsSource(ca, 128, prefix.utf8ToString());
    }

    public static IntervalsSource multiterm(CompiledAutomaton ca, String label) {
        return new MultiTermIntervalsSource(ca, 128, label);
    }

    static class MultiTermIntervalsSource extends IntervalsSource {

        private final CompiledAutomaton automaton;
        private final int maxExpansions;
        private final String pattern;

        MultiTermIntervalsSource(CompiledAutomaton automaton, int maxExpansions, String pattern) {
            this.automaton = automaton;
            if (maxExpansions > BooleanQuery.getMaxClauseCount()) {
                throw new IllegalArgumentException("maxExpansions [" + maxExpansions
                    + "] cannot be greater than BooleanQuery.getMaxClauseCount [" + BooleanQuery.getMaxClauseCount() + "]");
            }
            this.maxExpansions = maxExpansions;
            this.pattern = pattern;
        }

        @Override
        public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
            Terms terms = ctx.reader().terms(field);
            if (terms == null) {
                return null;
            }
            List<IntervalIterator> subSources = new ArrayList<>();
            TermsEnum te = automaton.getTermsEnum(terms);
            BytesRef term;
            int count = 0;
            while ((term = te.next()) != null) {
                subSources.add(TermIntervalsSource.intervals(term, te));
                if (++count > maxExpansions) {
                    throw new IllegalStateException("Automaton [" + this.pattern + "] expanded to too many terms (limit "
                        + maxExpansions + ")");
                }
            }
            if (subSources.size() == 0) {
                return null;
            }
            return new DisjunctionIntervalIterator(subSources);
        }

        @Override
        public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
            Terms terms = ctx.reader().terms(field);
            if (terms == null) {
                return null;
            }
            List<MatchesIterator> subMatches = new ArrayList<>();
            TermsEnum te = automaton.getTermsEnum(terms);
            BytesRef term;
            int count = 0;
            while ((term = te.next()) != null) {
                MatchesIterator mi = XIntervals.TermIntervalsSource.matches(te, doc);
                if (mi != null) {
                    subMatches.add(mi);
                    if (count++ > maxExpansions) {
                        throw new IllegalStateException("Automaton " + term + " expanded to too many terms (limit " + maxExpansions + ")");
                    }
                }
            }
            MatchesIterator mi = MatchesUtils.disjunction(subMatches);
            if (mi == null) {
                return null;
            }
            return new IntervalMatchesIterator() {
                @Override
                public int gaps() {
                    return 0;
                }

                @Override
                public int width() {
                    return 1;
                }

                @Override
                public boolean next() throws IOException {
                    return mi.next();
                }

                @Override
                public int startPosition() {
                    return mi.startPosition();
                }

                @Override
                public int endPosition() {
                    return mi.endPosition();
                }

                @Override
                public int startOffset() throws IOException {
                    return mi.startOffset();
                }

                @Override
                public int endOffset() throws IOException {
                    return mi.endOffset();
                }

                @Override
                public MatchesIterator getSubMatches() throws IOException {
                    return mi.getSubMatches();
                }

                @Override
                public Query getQuery() {
                    return mi.getQuery();
                }
            };
        }

        @Override
        public void visit(String field, QueryVisitor visitor) {
            visitor.visitLeaf(new IntervalQuery(field, this));
        }

        @Override
        public int minExtent() {
            return 1;
        }

        @Override
        public Collection<IntervalsSource> pullUpDisjunctions() {
            return Collections.singleton(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MultiTermIntervalsSource that = (MultiTermIntervalsSource) o;
            return maxExpansions == that.maxExpansions &&
                Objects.equals(automaton, that.automaton) &&
                Objects.equals(pattern, that.pattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(automaton, maxExpansions, pattern);
        }

        @Override
        public String toString() {
            return "MultiTerm(" + pattern + ")";
        }
    }

    static class DisiWrapper {

        public final DocIdSetIterator iterator;
        public final IntervalIterator intervals;
        public final long cost;
        public final float matchCost; // the match cost for two-phase iterators, 0 otherwise
        public int doc; // the current doc, used for comparison
        public DisiWrapper next; // reference to a next element, see #topList

        // An approximation of the iterator, or the iterator itself if it does not
        // support two-phase iteration
        public final DocIdSetIterator approximation;

        DisiWrapper(IntervalIterator iterator) {
            this.intervals = iterator;
            this.iterator = iterator;
            this.cost = iterator.cost();
            this.doc = -1;
            this.approximation = iterator;
            this.matchCost = iterator.matchCost();
        }

    }

    static final class DisiPriorityQueue implements Iterable<DisiWrapper> {

        static int leftNode(int node) {
            return ((node + 1) << 1) - 1;
        }

        static int rightNode(int leftNode) {
            return leftNode + 1;
        }

        static int parentNode(int node) {
            return ((node + 1) >>> 1) - 1;
        }

        private final DisiWrapper[] heap;
        private int size;

        DisiPriorityQueue(int maxSize) {
            heap = new DisiWrapper[maxSize];
            size = 0;
        }

        public int size() {
            return size;
        }

        public DisiWrapper top() {
            return heap[0];
        }

        /** Get the list of scorers which are on the current doc. */
        DisiWrapper topList() {
            final DisiWrapper[] heap = this.heap;
            final int size = this.size;
            DisiWrapper list = heap[0];
            list.next = null;
            if (size >= 3) {
                list = topList(list, heap, size, 1);
                list = topList(list, heap, size, 2);
            } else if (size == 2 && heap[1].doc == list.doc) {
                list = prepend(heap[1], list);
            }
            return list;
        }

        // prepend w1 (iterator) to w2 (list)
        private DisiWrapper prepend(DisiWrapper w1, DisiWrapper w2) {
            w1.next = w2;
            return w1;
        }

        private DisiWrapper topList(DisiWrapper list, DisiWrapper[] heap,
                                    int size, int i) {
            final DisiWrapper w = heap[i];
            if (w.doc == list.doc) {
                list = prepend(w, list);
                final int left = leftNode(i);
                final int right = left + 1;
                if (right < size) {
                    list = topList(list, heap, size, left);
                    list = topList(list, heap, size, right);
                } else if (left < size && heap[left].doc == list.doc) {
                    list = prepend(heap[left], list);
                }
            }
            return list;
        }

        public DisiWrapper add(DisiWrapper entry) {
            final DisiWrapper[] heap = this.heap;
            final int size = this.size;
            heap[size] = entry;
            upHeap(size);
            this.size = size + 1;
            return heap[0];
        }

        public DisiWrapper pop() {
            final DisiWrapper[] heap = this.heap;
            final DisiWrapper result = heap[0];
            final int i = --size;
            heap[0] = heap[i];
            heap[i] = null;
            downHeap(i);
            return result;
        }

        DisiWrapper updateTop() {
            downHeap(size);
            return heap[0];
        }

        void upHeap(int i) {
            final DisiWrapper node = heap[i];
            final int nodeDoc = node.doc;
            int j = parentNode(i);
            while (j >= 0 && nodeDoc < heap[j].doc) {
                heap[i] = heap[j];
                i = j;
                j = parentNode(j);
            }
            heap[i] = node;
        }

        void downHeap(int size) {
            int i = 0;
            final DisiWrapper node = heap[0];
            int j = leftNode(i);
            if (j < size) {
                int k = rightNode(j);
                if (k < size && heap[k].doc < heap[j].doc) {
                    j = k;
                }
                if (heap[j].doc < node.doc) {
                    do {
                        heap[i] = heap[j];
                        i = j;
                        j = leftNode(i);
                        k = rightNode(j);
                        if (k < size && heap[k].doc < heap[j].doc) {
                            j = k;
                        }
                    } while (j < size && heap[j].doc < node.doc);
                    heap[i] = node;
                }
            }
        }

        @Override
        public Iterator<DisiWrapper> iterator() {
            return Arrays.asList(heap).subList(0, size).iterator();
        }

    }

    static class DisjunctionDISIApproximation extends DocIdSetIterator {

        final DisiPriorityQueue subIterators;
        final long cost;

        DisjunctionDISIApproximation(DisiPriorityQueue subIterators) {
            this.subIterators = subIterators;
            long cost = 0;
            for (DisiWrapper w : subIterators) {
                cost += w.cost;
            }
            this.cost = cost;
        }

        @Override
        public long cost() {
            return cost;
        }

        @Override
        public int docID() {
            return subIterators.top().doc;
        }

        @Override
        public int nextDoc() throws IOException {
            DisiWrapper top = subIterators.top();
            final int doc = top.doc;
            do {
                top.doc = top.approximation.nextDoc();
                top = subIterators.updateTop();
            } while (top.doc == doc);

            return top.doc;
        }

        @Override
        public int advance(int target) throws IOException {
            DisiWrapper top = subIterators.top();
            do {
                top.doc = top.approximation.advance(target);
                top = subIterators.updateTop();
            } while (top.doc < target);

            return top.doc;
        }
    }

    static class DisjunctionIntervalIterator extends IntervalIterator {

        final DocIdSetIterator approximation;
        final PriorityQueue<IntervalIterator> intervalQueue;
        final DisiPriorityQueue disiQueue;
        final List<IntervalIterator> iterators;
        final float matchCost;

        IntervalIterator current = EMPTY;

        DisjunctionIntervalIterator(List<IntervalIterator> iterators) {
            this.disiQueue = new DisiPriorityQueue(iterators.size());
            for (IntervalIterator it : iterators) {
                disiQueue.add(new DisiWrapper(it));
            }
            this.approximation = new DisjunctionDISIApproximation(disiQueue);
            this.iterators = iterators;
            this.intervalQueue = new PriorityQueue<>(iterators.size()) {
                @Override
                protected boolean lessThan(IntervalIterator a, IntervalIterator b) {
                    return a.end() < b.end() || (a.end() == b.end() && a.start() >= b.start());
                }
            };
            float costsum = 0;
            for (IntervalIterator it : iterators) {
                costsum += it.cost();
            }
            this.matchCost = costsum;
        }

        @Override
        public float matchCost() {
            return matchCost;
        }

        @Override
        public int start() {
            return current.start();
        }

        @Override
        public int end() {
            return current.end();
        }

        @Override
        public int gaps() {
            return current.gaps();
        }

        private void reset() throws IOException {
            intervalQueue.clear();
            for (DisiWrapper dw = disiQueue.topList(); dw != null; dw = dw.next) {
                dw.intervals.nextInterval();
                intervalQueue.add(dw.intervals);
            }
            current = EMPTY;
        }

        @Override
        public int nextInterval() throws IOException {
            if (current == EMPTY || current == EXHAUSTED) {
                if (intervalQueue.size() > 0) {
                    current = intervalQueue.top();
                }
                return current.start();
            }
            int start = current.start(), end = current.end();
            while (intervalQueue.size() > 0 && contains(intervalQueue.top(), start, end)) {
                IntervalIterator it = intervalQueue.pop();
                if (it != null && it.nextInterval() != NO_MORE_INTERVALS) {
                    intervalQueue.add(it);
                }
            }
            if (intervalQueue.size() == 0) {
                current = EXHAUSTED;
                return NO_MORE_INTERVALS;
            }
            current = intervalQueue.top();
            return current.start();
        }

        private boolean contains(IntervalIterator it, int start, int end) {
            return start >= it.start() && start <= it.end() && end >= it.start() && end <= it.end();
        }

        @Override
        public int docID() {
            return approximation.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = approximation.nextDoc();
            reset();
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = approximation.advance(target);
            reset();
            return doc;
        }

        @Override
        public long cost() {
            return approximation.cost();
        }
    }

    private static final IntervalIterator EMPTY = new IntervalIterator() {

        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextDoc() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int start() {
            return -1;
        }

        @Override
        public int end() {
            return -1;
        }

        @Override
        public int gaps() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextInterval() {
            return NO_MORE_INTERVALS;
        }

        @Override
        public float matchCost() {
            return 0;
        }
    };

    private static final IntervalIterator EXHAUSTED = new IntervalIterator() {

        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextDoc() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int start() {
            return NO_MORE_INTERVALS;
        }

        @Override
        public int end() {
            return NO_MORE_INTERVALS;
        }

        @Override
        public int gaps() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextInterval() {
            return NO_MORE_INTERVALS;
        }

        @Override
        public float matchCost() {
            return 0;
        }
    };

    static class TermIntervalsSource extends IntervalsSource {

        final BytesRef term;

        TermIntervalsSource(BytesRef term) {
            this.term = term;
        }

        @Override
        public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
            Terms terms = ctx.reader().terms(field);
            if (terms == null)
                return null;
            if (terms.hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create an IntervalIterator over field " + field
                    + " because it has no indexed positions");
            }
            TermsEnum te = terms.iterator();
            if (te.seekExact(term) == false) {
                return null;
            }
            return intervals(term, te);
        }

        static IntervalIterator intervals(BytesRef term, TermsEnum te) throws IOException {
            PostingsEnum pe = te.postings(null, PostingsEnum.POSITIONS);
            float cost = termPositionsCost(te);
            return new IntervalIterator() {

                @Override
                public int docID() {
                    return pe.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    int doc = pe.nextDoc();
                    reset();
                    return doc;
                }

                @Override
                public int advance(int target) throws IOException {
                    int doc = pe.advance(target);
                    reset();
                    return doc;
                }

                @Override
                public long cost() {
                    return pe.cost();
                }

                int pos = -1, upto;

                @Override
                public int start() {
                    return pos;
                }

                @Override
                public int end() {
                    return pos;
                }

                @Override
                public int gaps() {
                    return 0;
                }

                @Override
                public int nextInterval() throws IOException {
                    if (upto <= 0)
                        return pos = NO_MORE_INTERVALS;
                    upto--;
                    return pos = pe.nextPosition();
                }

                @Override
                public float matchCost() {
                    return cost;
                }

                private void reset() throws IOException {
                    if (pe.docID() == NO_MORE_DOCS) {
                        upto = -1;
                        pos = NO_MORE_INTERVALS;
                    }
                    else {
                        upto = pe.freq();
                        pos = -1;
                    }
                }

                @Override
                public String toString() {
                    return term.utf8ToString() + ":" + super.toString();
                }
            };
        }

        @Override
        public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
            Terms terms = ctx.reader().terms(field);
            if (terms == null)
                return null;
            if (terms.hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create an IntervalIterator over field " + field
                    + " because it has no indexed positions");
            }
            TermsEnum te = terms.iterator();
            if (te.seekExact(term) == false) {
                return null;
            }
            return matches(te, doc);
        }

        static IntervalMatchesIterator matches(TermsEnum te, int doc) throws IOException {
            PostingsEnum pe = te.postings(null, PostingsEnum.OFFSETS);
            if (pe.advance(doc) != doc) {
                return null;
            }
            return new IntervalMatchesIterator() {

                @Override
                public int gaps() {
                    return 0;
                }

                @Override
                public int width() {
                    return 1;
                }

                int upto = pe.freq();
                int pos = -1;

                @Override
                public boolean next() throws IOException {
                    if (upto <= 0) {
                        pos = IntervalIterator.NO_MORE_INTERVALS;
                        return false;
                    }
                    upto--;
                    pos = pe.nextPosition();
                    return true;
                }

                @Override
                public int startPosition() {
                    return pos;
                }

                @Override
                public int endPosition() {
                    return pos;
                }

                @Override
                public int startOffset() throws IOException {
                    return pe.startOffset();
                }

                @Override
                public int endOffset() throws IOException {
                    return pe.endOffset();
                }

                @Override
                public MatchesIterator getSubMatches() {
                    return null;
                }

                @Override
                public Query getQuery() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int minExtent() {
            return 1;
        }

        @Override
        public Collection<IntervalsSource> pullUpDisjunctions() {
            return Collections.singleton(this);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TermIntervalsSource that = (TermIntervalsSource) o;
            return Objects.equals(term, that.term);
        }

        @Override
        public String toString() {
            return term.utf8ToString();
        }

        @Override
        public void visit(String field, QueryVisitor visitor) {
            visitor.consumeTerms(new IntervalQuery(field, this), new Term(field, term));
        }

        private static final int TERM_POSNS_SEEK_OPS_PER_DOC = 128;

        private static final int TERM_OPS_PER_POS = 7;

        static float termPositionsCost(TermsEnum termsEnum) throws IOException {
            int docFreq = termsEnum.docFreq();
            assert docFreq > 0;
            long totalTermFreq = termsEnum.totalTermFreq();
            float expOccurrencesInMatchingDoc = totalTermFreq / (float) docFreq;
            return TERM_POSNS_SEEK_OPS_PER_DOC + expOccurrencesInMatchingDoc * TERM_OPS_PER_POS;
        }
    }

}
