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

import com.google.common.primitives.Ints;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
 * {@link org.apache.lucene.search.similarities.DefaultSimilarity}
 * can only be estimated as an lower-bound since it is a document based statistic. For
 * the document frequency the maximum frequency across all fields per term is used
 * which is the minimum number of documents the terms occurs in.
 * </p>
 */
// TODO maybe contribute to Lucene
public abstract class BlendedTermQuery extends Query {

    private final Term[] terms;


    public BlendedTermQuery(Term[] terms) {
        if (terms == null || terms.length == 0) {
            throw new IllegalArgumentException("terms must not be null or empty");
        }
        this.terms = terms;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        IndexReaderContext context = reader.getContext();
        TermContext[] ctx = new TermContext[terms.length];
        int[] docFreqs = new int[ctx.length];
        for (int i = 0; i < terms.length; i++) {
            ctx[i] = TermContext.build(context, terms[i]);
            docFreqs[i] = ctx[i].docFreq();
        }

        final int maxDoc = reader.maxDoc();
        blend(ctx, maxDoc, reader);
        Query query = topLevelQuery(terms, ctx, docFreqs, maxDoc);
        query.setBoost(getBoost());
        return query;
    }

    protected abstract Query topLevelQuery(Term[] terms, TermContext[] ctx, int[] docFreqs, int maxDoc);

    protected void blend(final TermContext[] contexts, int maxDoc, IndexReader reader) throws IOException {
        if (contexts.length <= 1) {
            return;
        }
        int max = 0;
        long minSumTTF = Long.MAX_VALUE;
        for (int i = 0; i < contexts.length; i++) {
            TermContext ctx = contexts[i];
            int df = ctx.docFreq();
            // we use the max here since it's the only "true" estimation we can make here
            // at least max(df) documents have that term. Sum or Averages don't seem
            // to have a significant meaning here.
            // TODO: Maybe it could also make sense to assume independent distributions of documents and eg. have:
            //   df = df1 + df2 - (df1 * df2 / maxDoc)?
            max = Math.max(df, max);
            if (minSumTTF != -1 && ctx.totalTermFreq() != -1) {
                // we need to find out the minimum sumTTF to adjust the statistics
                // otherwise the statistics don't match
                minSumTTF = Math.min(minSumTTF, reader.getSumTotalTermFreq(terms[i].field()));
            } else {
                minSumTTF = -1;
            }

        }
        if (minSumTTF != -1 && maxDoc > minSumTTF) {
            maxDoc = (int)minSumTTF;
        }

        if (max == 0) {
            return; // we are done that term doesn't exist at all
        }
        long sumTTF = minSumTTF == -1 ? -1 : 0;
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
                return Ints.compare(contexts[tieBreak[j]].docFreq(), contexts[tieBreak[i]].docFreq());
            }
        }.sort(0, tieBreak.length);
        int prev = contexts[tieBreak[0]].docFreq();
        int actualDf = Math.min(maxDoc, max);
        assert actualDf >=0 : "DF must be >= 0";


        // here we try to add a little bias towards
        // the more popular (more frequent) fields
        // that acts as a tie breaker
        for (int i : tieBreak) {
            TermContext ctx = contexts[i];
            if (ctx.docFreq() == 0) {
                break;
            }
            final int current = ctx.docFreq();
            if (prev > current) {
                actualDf++;
            }
            contexts[i] = ctx = adjustDF(ctx, Math.min(maxDoc, actualDf));
            prev = current;
            if (sumTTF >= 0 && ctx.totalTermFreq() >= 0) {
                sumTTF += ctx.totalTermFreq();
            } else {
                sumTTF = -1;  // omit once TF is omitted anywhere!
            }
        }
        sumTTF = Math.min(sumTTF, minSumTTF);
        for (int i = 0; i < contexts.length; i++) {
            int df = contexts[i].docFreq();
            if (df == 0) {
                continue;
            }
            // the blended sumTTF can't be greater than the sumTTTF on the field
            final long fixedTTF = sumTTF == -1 ? -1 : sumTTF;
            contexts[i] = adjustTTF(contexts[i], fixedTTF);
        }
    }

    private TermContext adjustTTF(TermContext termContext, long sumTTF) {
        if (sumTTF == -1 && termContext.totalTermFreq() == -1) {
            return termContext;
        }
        TermContext newTermContext = new TermContext(termContext.topReaderContext);
        List<LeafReaderContext> leaves = termContext.topReaderContext.leaves();
        final int len;
        if (leaves == null) {
            len = 1;
        } else {
            len = leaves.size();
        }
        int df = termContext.docFreq();
        long ttf = sumTTF;
        for (int i = 0; i < len; i++) {
            TermState termState = termContext.get(i);
            if (termState == null) {
                continue;
            }
            newTermContext.register(termState, i, df, ttf);
            df = 0;
            ttf = 0;
        }
        return newTermContext;
    }

    private static TermContext adjustDF(TermContext ctx, int newDocFreq) {
        // Use a value of ttf that is consistent with the doc freq (ie. gte)
        long newTTF;
        if (ctx.totalTermFreq() < 0) {
            newTTF = -1;
        } else {
            newTTF = Math.max(ctx.totalTermFreq(), newDocFreq);
        }
        List<LeafReaderContext> leaves = ctx.topReaderContext.leaves();
        final int len;
        if (leaves == null) {
            len = 1;
        } else {
            len = leaves.size();
        }
        TermContext newCtx = new TermContext(ctx.topReaderContext);
        for (int i = 0; i < len; ++i) {
            TermState termState = ctx.get(i);
            if (termState == null) {
                continue;
            }
            newCtx.register(termState, i, newDocFreq, newTTF);
            newDocFreq = 0;
            newTTF = 0;
        }
        return newCtx;
    }

    @Override
    public String toString(String field) {
        return "blended(terms: " + Arrays.toString(terms) + ")";

    }

    private volatile Term[] equalTerms = null;

    private Term[] equalsTerms() {
        if (terms.length == 1) {
            return terms;
        }
        if (equalTerms == null) {
            // sort the terms to make sure equals and hashCode are consistent
            // this should be a very small cost and equivalent to a HashSet but less object creation
            final Term[] t = new Term[terms.length];
            System.arraycopy(terms, 0, t, 0, terms.length);
            ArrayUtil.timSort(t);
            equalTerms = t;
        }
        return equalTerms;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BlendedTermQuery that = (BlendedTermQuery) o;
        if (!Arrays.equals(equalsTerms(), that.equalsTerms())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(equalsTerms());
        return result;
    }

    public static BlendedTermQuery booleanBlendedQuery(Term[] terms, final boolean disableCoord) {
        return booleanBlendedQuery(terms, null, disableCoord);
    }

    public static BlendedTermQuery booleanBlendedQuery(Term[] terms, final float[] boosts, final boolean disableCoord) {
        return new BlendedTermQuery(terms) {
            @Override
            protected Query topLevelQuery(Term[] terms, TermContext[] ctx, int[] docFreqs, int maxDoc) {
                BooleanQuery query = new BooleanQuery(disableCoord);
                for (int i = 0; i < terms.length; i++) {
                    TermQuery termQuery = new TermQuery(terms[i], ctx[i]);
                    if (boosts != null) {
                        termQuery.setBoost(boosts[i]);
                    }
                    query.add(termQuery, BooleanClause.Occur.SHOULD);
                }
                return query;
            }
        };
    }

    public static BlendedTermQuery commonTermsBlendedQuery(Term[] terms, final float[] boosts, final boolean disableCoord, final float maxTermFrequency) {
        return new BlendedTermQuery(terms) {
            @Override
            protected Query topLevelQuery(Term[] terms, TermContext[] ctx, int[] docFreqs, int maxDoc) {
                BooleanQuery query = new BooleanQuery(true);
                BooleanQuery high = new BooleanQuery(disableCoord);
                BooleanQuery low = new BooleanQuery(disableCoord);
                for (int i = 0; i < terms.length; i++) {
                    TermQuery termQuery = new TermQuery(terms[i], ctx[i]);
                    if (boosts != null) {
                        termQuery.setBoost(boosts[i]);
                    }
                    if ((maxTermFrequency >= 1f && docFreqs[i] > maxTermFrequency)
                            || (docFreqs[i] > (int) Math.ceil(maxTermFrequency
                            * (float) maxDoc))) {
                        high.add(termQuery, BooleanClause.Occur.SHOULD);
                    } else {
                        low.add(termQuery, BooleanClause.Occur.SHOULD);
                    }
                }
                if (low.clauses().isEmpty()) {
                    for (BooleanClause booleanClause : high) {
                        booleanClause.setOccur(BooleanClause.Occur.MUST);
                    }
                    return high;
                } else if (high.clauses().isEmpty()) {
                    return low;
                } else {
                    query.add(high, BooleanClause.Occur.SHOULD);
                    query.add(low, BooleanClause.Occur.MUST);
                    return query;
                }
            }
        };
    }

    public static BlendedTermQuery dismaxBlendedQuery(Term[] terms, final float tieBreakerMultiplier) {
        return dismaxBlendedQuery(terms, null, tieBreakerMultiplier);
    }

    public static BlendedTermQuery dismaxBlendedQuery(Term[] terms, final float[] boosts, final float tieBreakerMultiplier) {
        return new BlendedTermQuery(terms) {
            @Override
            protected Query topLevelQuery(Term[] terms, TermContext[] ctx, int[] docFreqs, int maxDoc) {
                DisjunctionMaxQuery query = new DisjunctionMaxQuery(tieBreakerMultiplier);
                for (int i = 0; i < terms.length; i++) {
                    TermQuery termQuery = new TermQuery(terms[i], ctx[i]);
                    if (boosts != null) {
                        termQuery.setBoost(boosts[i]);
                    }
                    query.add(termQuery);
                }
                return query;
            }
        };
    }
}
