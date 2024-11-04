/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A {@code RankDocsQuery} returns the top k documents in the order specified by the global doc IDs.
 * This is used by retrievers that compute a score for a large document set, and need access to just the top results,
 * after performing any reranking or filtering.
 */
public class RankDocsQuery extends Query {
    /**
     * A {@link Query} that matches only the specified {@link RankDoc}, using the provided {@link Query} sources
     * solely for the purpose of explainability.
     */
    public static class TopQuery extends Query {
        private final RankDoc[] docs;
        private final Query[] sources;
        private final String[] queryNames;
        private final int[] segmentStarts;
        private final Object contextIdentity;

        TopQuery(RankDoc[] docs, Query[] sources, String[] queryNames, int[] segmentStarts, Object contextIdentity) {
            assert sources.length == queryNames.length;
            this.docs = docs;
            this.sources = sources;
            this.queryNames = queryNames;
            this.segmentStarts = segmentStarts;
            this.contextIdentity = contextIdentity;
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            if (docs.length == 0) {
                return new MatchNoDocsQuery();
            }
            boolean changed = false;
            Query[] newSources = new Query[sources.length];
            for (int i = 0; i < sources.length; i++) {
                newSources[i] = sources[i].rewrite(searcher);
                changed |= newSources[i] != sources[i];
            }
            if (changed) {
                return new TopQuery(docs, newSources, queryNames, segmentStarts, contextIdentity);
            }
            return this;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            if (searcher.getIndexReader().getContext().id() != contextIdentity) {
                throw new IllegalStateException("This RankDocsDocQuery was created by a different reader");
            }
            Weight[] weights = new Weight[sources.length];
            for (int i = 0; i < sources.length; i++) {
                weights[i] = sources[i].createWeight(searcher, scoreMode, boost);
            }
            return new Weight(this) {
                @Override
                public int count(LeafReaderContext context) {
                    return segmentStarts[context.ord + 1] - segmentStarts[context.ord];
                }

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    int found = binarySearch(docs, 0, docs.length, doc + context.docBase);
                    if (found < 0) {
                        return Explanation.noMatch("doc not found in top " + docs.length + " rank docs");
                    }
                    Explanation[] sourceExplanations = new Explanation[sources.length];
                    for (int i = 0; i < sources.length; i++) {
                        sourceExplanations[i] = weights[i].explain(context, doc);
                    }
                    return docs[found].explain(sourceExplanations, queryNames);
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    /**
                     * We return a scorer even if there are no ranked documents within the segment.
                     * This ensures the correct propagation of the maximum score.
                     */
                    Scorer scorer = new Scorer() {
                        final int lower = segmentStarts[context.ord];
                        final int upper = segmentStarts[context.ord + 1];
                        int upTo = -1;
                        float score;

                        @Override
                        public DocIdSetIterator iterator() {
                            return new DocIdSetIterator() {
                                @Override
                                public int docID() {
                                    return currentDocId();
                                }

                                @Override
                                public int nextDoc() {
                                    if (upTo == -1) {
                                        upTo = lower;
                                    } else {
                                        ++upTo;
                                    }
                                    return currentDocId();
                                }

                                @Override
                                public int advance(int target) throws IOException {
                                    return slowAdvance(target);
                                }

                                @Override
                                public long cost() {
                                    return upper - lower;
                                }
                            };
                        }

                        @Override
                        public float getMaxScore(int docId) {
                            if (docId != NO_MORE_DOCS) {
                                docId += context.docBase;
                            }
                            float maxScore = 0;
                            for (int idx = Math.max(lower, upTo); idx < upper && docs[idx].doc <= docId; idx++) {
                                maxScore = Math.max(maxScore, docs[idx].score);
                            }
                            return maxScore;
                        }

                        @Override
                        public float score() {
                            return docs[upTo].score;
                        }

                        @Override
                        public int docID() {
                            return currentDocId();
                        }

                        private int currentDocId() {
                            if (upTo == -1) {
                                return -1;
                            }
                            if (upTo >= upper) {
                                return NO_MORE_DOCS;
                            }
                            return docs[upTo].doc - context.docBase;
                        }

                    };
                    return new DefaultScorerSupplier(scorer);
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

        @Override
        public String toString(String field) {
            return this.getClass().getSimpleName() + "{rank_docs:" + Arrays.toString(docs) + "}";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj) == false) {
                return false;
            }
            TopQuery other = (TopQuery) obj;
            return Arrays.equals(docs, other.docs)
                && Arrays.equals(segmentStarts, other.segmentStarts)
                && contextIdentity == other.contextIdentity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), Arrays.hashCode(docs), Arrays.hashCode(segmentStarts), contextIdentity);
        }
    }

    private final RankDoc[] docs;
    // topQuery is used to match just the top docs from all the original queries. This match is based on the RankDoc array
    // provided when constructing the object. This is the only clause that actually contributes to scoring
    private final Query topQuery;
    // tailQuery is used to match <b>all</b> the original documents that were used to compute the top docs.
    // This is useful if we want to compute aggregations, total hits etc based on all matching documents, and not just the top
    // RankDocs provided. This query does not contribute to scoring, as it is set as filter when creating the weight
    private final Query tailQuery;
    private final boolean onlyRankDocs;

    /**
     * Creates a {@code RankDocsQuery} based on the provided docs.
     *
     * @param rankDocs     The global doc IDs of documents that match, in ascending order
     * @param sources      The original queries that were used to compute the top documents
     * @param queryNames   The names (if present) of the original retrievers
     * @param onlyRankDocs Whether the query should only match the provided rank docs
     */
    public RankDocsQuery(IndexReader reader, RankDoc[] rankDocs, Query[] sources, String[] queryNames, boolean onlyRankDocs) {
        assert sources.length == queryNames.length;
        // clone to avoid side-effect after sorting
        this.docs = rankDocs.clone();
        // sort rank docs by doc id
        Arrays.sort(docs, Comparator.comparingInt(a -> a.doc));
        this.topQuery = new TopQuery(docs, sources, queryNames, findSegmentStarts(reader, docs), reader.getContext().id());
        if (sources.length > 0 && false == onlyRankDocs) {
            var bq = new BooleanQuery.Builder();
            for (var source : sources) {
                bq.add(source, BooleanClause.Occur.SHOULD);
            }
            this.tailQuery = bq.build();
        } else {
            this.tailQuery = null;
        }
        this.onlyRankDocs = onlyRankDocs;
    }

    private RankDocsQuery(RankDoc[] docs, Query topQuery, Query tailQuery, boolean onlyRankDocs) {
        this.docs = docs;
        this.topQuery = topQuery;
        this.tailQuery = tailQuery;
        this.onlyRankDocs = onlyRankDocs;
    }

    private static int binarySearch(RankDoc[] docs, int fromIndex, int toIndex, int key) {
        return Arrays.binarySearch(docs, fromIndex, toIndex, new RankDoc(key, Float.NaN, -1), Comparator.comparingInt(a -> a.doc));
    }

    private static int[] findSegmentStarts(IndexReader reader, RankDoc[] docs) {
        int[] starts = new int[reader.leaves().size() + 1];
        starts[starts.length - 1] = docs.length;
        if (starts.length == 2) {
            return starts;
        }
        int resultIndex = 0;
        for (int i = 1; i < starts.length - 1; i++) {
            int upper = reader.leaves().get(i).docBase;

            resultIndex = binarySearch(docs, resultIndex, docs.length, upper);
            if (resultIndex < 0) {
                resultIndex = -1 - resultIndex;
            }
            starts[i] = resultIndex;
        }
        return starts;
    }

    RankDoc[] rankDocs() {
        return docs;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (tailQuery == null) {
            return topQuery;
        }
        boolean hasChanged = false;
        var topRewrite = topQuery.rewrite(searcher);
        if (topRewrite != topQuery) {
            hasChanged = true;
        }
        var tailRewrite = tailQuery.rewrite(searcher);
        if (tailRewrite != tailQuery) {
            hasChanged = true;
        }
        return hasChanged ? new RankDocsQuery(docs, topRewrite, tailRewrite, onlyRankDocs) : this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (tailQuery == null) {
            throw new IllegalArgumentException("[tailQuery] should not be null; maybe missing a rewrite?");
        }
        var combined = new BooleanQuery.Builder().add(topQuery, onlyRankDocs ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD)
            .add(tailQuery, BooleanClause.Occur.FILTER)
            .build();
        var topWeight = topQuery.createWeight(searcher, scoreMode, boost);
        var combinedWeight = searcher.rewrite(combined).createWeight(searcher, scoreMode, boost);
        return new Weight(this) {
            @Override
            public int count(LeafReaderContext context) throws IOException {
                return combinedWeight.count(context);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return topWeight.explain(context, doc);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return combinedWeight.isCacheable(ctx);
            }

            @Override
            public Matches matches(LeafReaderContext context, int doc) throws IOException {
                return combinedWeight.matches(context, doc);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                return combinedWeight.scorerSupplier(context);
            }
        };
    }

    @Override
    public String toString(String field) {
        return this.getClass().getSimpleName() + "{rank_docs:" + Arrays.toString(docs) + "}";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        topQuery.visit(visitor);
        if (tailQuery != null) {
            tailQuery.visit(visitor);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        RankDocsQuery other = (RankDocsQuery) obj;
        return Objects.equals(topQuery, other.topQuery) && Objects.equals(tailQuery, other.tailQuery) && onlyRankDocs == other.onlyRankDocs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), topQuery, tailQuery, onlyRankDocs);
    }
}
