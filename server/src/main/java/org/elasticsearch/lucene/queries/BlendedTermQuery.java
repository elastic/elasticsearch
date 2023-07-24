/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * BlendedTermQuery can be used to unify term statistics across
 * one or more fields in the index. A common problem with structured
 * documents is that a term that is significant in on field might not be
 * significant in other fields like in a scenario where documents represent
 * users with a "first_name" and a "second_name". When someone searches
 * for "simon" it will very likely get "paul simon" first since "simon" is a
 * an uncommon last name ie. has a low document frequency. This query
 * tries to "lie" about the global statistics like document frequency as well
 * total term frequency to rank based on the estimated statistics.
 * <p>
 * While aggregating the total term frequency is trivial since it
 * can be summed up not every {@link org.apache.lucene.search.similarities.Similarity}
 * makes use of this statistic. The document frequency which is used in the
 * {@link org.apache.lucene.search.similarities.ClassicSimilarity}
 * can only be estimated as an lower-bound since it is a document based statistic. For
 * the document frequency the maximum frequency across all fields per term is used
 * which is the minimum number of documents the terms occurs in.
 * </p>
 */
public abstract class BlendedTermQuery extends Query {

    private final Term[] terms;
    private final float[] boosts;

    public BlendedTermQuery(Term[] terms, float[] boosts) {
        if (terms == null || terms.length == 0) {
            throw new IllegalArgumentException("terms must not be null or empty");
        }
        if (boosts != null && boosts.length != terms.length) {
            throw new IllegalArgumentException("boosts must have the same size as terms");
        }
        this.terms = terms;
        this.boosts = boosts;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = super.rewrite(searcher);
        if (rewritten != this) {
            return rewritten;
        }
        IndexReader reader = searcher.getIndexReader();
        IndexReaderContext context = reader.getContext();
        TermStates[] ctx = new TermStates[terms.length];
        int[] docFreqs = new int[ctx.length];
        for (int i = 0; i < terms.length; i++) {
            ctx[i] = TermStates.build(context, terms[i], true);
            docFreqs[i] = ctx[i].docFreq();
        }

        final int maxDoc = reader.maxDoc();
        blend(ctx, maxDoc, reader);
        return topLevelQuery(terms, ctx, docFreqs, maxDoc);
    }

    protected abstract Query topLevelQuery(Term[] terms, TermStates[] ctx, int[] docFreqs, int maxDoc);

    protected void blend(final TermStates[] contexts, int maxDoc, IndexReader reader) throws IOException {
        if (contexts.length <= 1) {
            return;
        }
        int max = 0;
        long minSumTTF = Long.MAX_VALUE;
        for (int i = 0; i < contexts.length; i++) {
            TermStates ctx = contexts[i];
            int df = ctx.docFreq();
            // we use the max here since it's the only "true" estimation we can make here
            // at least max(df) documents have that term. Sum or Averages don't seem
            // to have a significant meaning here.
            // TODO: Maybe it could also make sense to assume independent distributions of documents and eg. have:
            // df = df1 + df2 - (df1 * df2 / maxDoc)?
            max = Math.max(df, max);
            if (ctx.totalTermFreq() > 0) {
                // we need to find out the minimum sumTTF to adjust the statistics
                // otherwise the statistics don't match
                minSumTTF = Math.min(minSumTTF, reader.getSumTotalTermFreq(terms[i].field()));
            }
        }
        if (maxDoc > minSumTTF) {
            maxDoc = (int) minSumTTF;
        }
        if (max == 0) {
            return; // we are done that term doesn't exist at all
        }
        long sumTTF = 0;
        final int[] tieBreak = new int[contexts.length];
        for (int i = 0; i < tieBreak.length; ++i) {
            tieBreak[i] = i;
        }
        new InPlaceMergeSorter() {
            @Override
            protected void swap(int i, int j) {
                final int tmp = tieBreak[i];
                tieBreak[i] = tieBreak[j];
                tieBreak[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return Integer.compare(contexts[tieBreak[j]].docFreq(), contexts[tieBreak[i]].docFreq());
            }
        }.sort(0, tieBreak.length);
        int prev = contexts[tieBreak[0]].docFreq();
        int actualDf = Math.min(maxDoc, max);
        assert actualDf >= 0 : "DF must be >= 0";

        // here we try to add a little bias towards
        // the more popular (more frequent) fields
        // that acts as a tie breaker
        for (int i : tieBreak) {
            TermStates ctx = contexts[i];
            if (ctx.docFreq() == 0) {
                break;
            }
            final int current = ctx.docFreq();
            if (prev > current) {
                actualDf++;
            }

            int docCount = reader.getDocCount(terms[i].field());

            // IMPORTANT: we make two adjustments here to ensure the new document frequency is valid:
            // 1. We take a minimum with docCount, which is the total number of documents that contain
            // this field. The document frequency must always be less than the document count.
            // 2. We also take a minimum with maxDoc. Earlier, maxDoc is adjusted to the minimum of
            // maxDoc and minTTF. So taking the minimum ensures that the document frequency is never
            // greater than the total term frequency, which would be illegal.
            int newDocFreq = Math.min(Math.min(actualDf, docCount), maxDoc);

            contexts[i] = ctx = adjustDF(reader.getContext(), ctx, newDocFreq);
            prev = current;
            sumTTF += ctx.totalTermFreq();
        }
        sumTTF = Math.min(sumTTF, minSumTTF);
        for (int i = 0; i < contexts.length; i++) {
            int df = contexts[i].docFreq();
            if (df == 0) {
                continue;
            }
            contexts[i] = adjustTTF(reader.getContext(), contexts[i], sumTTF);
        }
    }

    private static TermStates adjustTTF(IndexReaderContext readerContext, TermStates termContext, long sumTTF) throws IOException {
        assert termContext.wasBuiltFor(readerContext);
        TermStates newTermContext = new TermStates(readerContext);
        List<LeafReaderContext> leaves = readerContext.leaves();
        final int len;
        if (leaves == null) {
            len = 1;
        } else {
            len = leaves.size();
        }
        int df = termContext.docFreq();
        long ttf = sumTTF;
        for (int i = 0; i < len; i++) {
            TermState termState = termContext.get(leaves.get(i));
            if (termState == null) {
                continue;
            }
            newTermContext.register(termState, i, df, ttf);
            df = 0;
            ttf = 0;
        }
        return newTermContext;
    }

    private static TermStates adjustDF(IndexReaderContext readerContext, TermStates ctx, int newDocFreq) throws IOException {
        assert ctx.wasBuiltFor(readerContext);
        // Use a value of ttf that is consistent with the doc freq (ie. gte)
        long newTTF = Math.max(ctx.totalTermFreq(), newDocFreq);
        List<LeafReaderContext> leaves = readerContext.leaves();
        final int len;
        if (leaves == null) {
            len = 1;
        } else {
            len = leaves.size();
        }
        TermStates newCtx = new TermStates(readerContext);
        for (int i = 0; i < len; ++i) {
            TermState termState = ctx.get(leaves.get(i));
            if (termState == null) {
                continue;
            }
            newCtx.register(termState, i, newDocFreq, newTTF);
            newDocFreq = 0;
            newTTF = 0;
        }
        return newCtx;
    }

    public List<Term> getTerms() {
        return Arrays.asList(terms);
    }

    @Override
    public String toString(String field) {
        StringBuilder builder = new StringBuilder("blended(terms:[");
        for (int i = 0; i < terms.length; ++i) {
            builder.append(terms[i]);
            float boost = 1f;
            if (boosts != null) {
                boost = boosts[i];
            }
            if (boost != 1f) {
                builder.append('^').append(boost);
            }
            builder.append(", ");
        }
        if (terms.length > 0) {
            builder.setLength(builder.length() - 2);
        }
        builder.append("])");
        return builder.toString();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        Set<String> fields = Arrays.stream(terms).map(Term::field).collect(Collectors.toUnmodifiableSet());
        for (String field : fields) {
            if (visitor.acceptField(field) == false) {
                return;
            }
        }
        visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this).consumeTerms(this, terms);
    }

    private static class TermAndBoost implements Comparable<TermAndBoost> {
        protected final Term term;
        protected float boost;

        protected TermAndBoost(Term term, float boost) {
            this.term = term;
            this.boost = boost;
        }

        @Override
        public int compareTo(TermAndBoost other) {
            int compareTo = term.compareTo(other.term);
            if (compareTo == 0) {
                compareTo = Float.compare(boost, other.boost);
            }
            return compareTo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof TermAndBoost == false) {
                return false;
            }

            TermAndBoost that = (TermAndBoost) o;
            return term.equals(that.term) && (Float.compare(boost, that.boost) == 0);
        }

        @Override
        public int hashCode() {
            return 31 * term.hashCode() + Float.hashCode(boost);
        }
    }

    private volatile TermAndBoost[] equalTermsAndBoosts = null;

    private TermAndBoost[] equalsTermsAndBoosts() {
        if (equalTermsAndBoosts != null) {
            return equalTermsAndBoosts;
        }
        if (terms.length == 1) {
            float boost = (boosts != null ? boosts[0] : 1f);
            equalTermsAndBoosts = new TermAndBoost[] { new TermAndBoost(terms[0], boost) };
        } else {
            // sort the terms to make sure equals and hashCode are consistent
            // this should be a very small cost and equivalent to a HashSet but less object creation
            equalTermsAndBoosts = new TermAndBoost[terms.length];
            for (int i = 0; i < terms.length; i++) {
                float boost = (boosts != null ? boosts[i] : 1f);
                equalTermsAndBoosts[i] = new TermAndBoost(terms[i], boost);
            }
            ArrayUtil.timSort(equalTermsAndBoosts);
        }
        return equalTermsAndBoosts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }

        BlendedTermQuery that = (BlendedTermQuery) o;
        return Arrays.equals(equalsTermsAndBoosts(), that.equalsTermsAndBoosts());

    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), Arrays.hashCode(equalsTermsAndBoosts()));
    }

    public static BlendedTermQuery dismaxBlendedQuery(Term[] terms, final float tieBreakerMultiplier) {
        return dismaxBlendedQuery(terms, null, tieBreakerMultiplier);
    }

    public static BlendedTermQuery dismaxBlendedQuery(Term[] terms, final float[] boosts, final float tieBreakerMultiplier) {
        return new BlendedTermQuery(terms, boosts) {
            @Override
            protected Query topLevelQuery(Term[] terms, TermStates[] ctx, int[] docFreqs, int maxDoc) {
                List<Query> queries = new ArrayList<>(ctx.length);
                for (int i = 0; i < terms.length; i++) {
                    Query query = new TermQuery(terms[i], ctx[i]);
                    if (boosts != null && boosts[i] != 1f) {
                        query = new BoostQuery(query, boosts[i]);
                    }
                    queries.add(query);
                }
                return new DisjunctionMaxQuery(queries, tieBreakerMultiplier);
            }
        };
    }
}
