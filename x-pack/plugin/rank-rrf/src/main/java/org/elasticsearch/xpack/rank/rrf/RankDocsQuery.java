import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.search.function.MinScoreScorer;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.query.RankDocsQueryBuilder.DEFAULT_MIN_SCORE;

/**
 * A {@code RankDocsQuery} returns the top k documents in the order specified by the global doc IDs.
 */
public class RankDocsQuery extends Query {
    private final RankDoc[] docs;
    private final Query topQuery;
    // RankDocs provided. This query does not contribute to scoring, as it is set as filter when creating the weight
    private final Query tailQuery;
    private final boolean onlyRankDocs;
    private final float minScore;

    public static class TopQuery extends Query {
        private final RankDoc[] docs;
        private final Query[] sources;
        private final String[] queryNames;
        private final int[] segmentStarts;
        private final Object contextIdentity;
        private final float minScore;

        TopQuery(RankDoc[] docs, Query[] sources, String[] queryNames, int[] segmentStarts, Object contextIdentity, float minScore) {
            assert sources.length == queryNames.length;
            this.docs = docs;
            this.sources = sources;
            this.queryNames = queryNames;
            this.segmentStarts = segmentStarts;
            this.contextIdentity = contextIdentity;
            this.minScore = minScore;
            for (RankDoc doc : docs) {
                if (false == doc.score >= 0) {
                    throw new IllegalArgumentException("RankDoc scores must be positive values. Missing a normalization step?");
                }
            }
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            Query[] newSources = new Query[sources.length];
            boolean changed = false;
            for (int i = 0; i < sources.length; i++) {
                newSources[i] = searcher.rewrite(sources[i]);
                changed |= newSources[i] != sources[i];
            }
            if (changed) {
                return new TopQuery(docs, newSources, queryNames, segmentStarts, contextIdentity, minScore);
            }
            return this;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new Weight(this) {
                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    return Explanation.match(0f, "Rank docs query does not explain");
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    final int docBase = context.docBase;
                    final int ord = Arrays.binarySearch(segmentStarts, docBase);
                    final int startOffset;
                    final int endOffset;
                    if (ord < 0) {
                        int insertion = -1 - ord;
                        if (insertion >= segmentStarts.length) {
                            return null;
                        }
                        startOffset = insertion - 1;
                        endOffset = insertion;
                    } else {
                        startOffset = ord - 1;
                        endOffset = ord;
                    }
                    final int start = segmentStarts[startOffset];
                    final int end = segmentStarts[endOffset];
                    if (start == end) {
                        return null;
                    }
                    return new Scorer(this) {
                        int upTo = start - 1;
                        final int docBound = end;

                        @Override
                        public DocIdSetIterator iterator() {
                            return new DocIdSetIterator() {
                                @Override
                                public int docID() {
                                    return upTo < start ? -1 : docs[upTo].doc;
                                }

                                @Override
                                public int nextDoc() throws IOException {
                                    if (++upTo >= docBound) {
                                        return NO_MORE_DOCS;
                                    }
                                    return docs[upTo].doc;
                                }

                                @Override
                                public int advance(int target) throws IOException {
                                    if (++upTo >= docBound) {
                                        return NO_MORE_DOCS;
                                    }
                                    upTo = Arrays.binarySearch(docs, upTo, docBound, new RankDoc(target, Float.NaN, -1), Comparator.comparingInt(a -> a.doc));
                                    if (upTo < 0) {
                                        upTo = -1 - upTo;
                                        if (upTo >= docBound) {
                                            return NO_MORE_DOCS;
                                        }
                                    }
                                    return docs[upTo].doc;
                                }

                                @Override
                                public long cost() {
                                    return docBound - upTo;
                                }
                            };
                        }

                        @Override
                        public float score() throws IOException {
                            // We need to handle scores of exactly 0 specially:
                            // Even when a document legitimately has a score of 0, we replace it with DEFAULT_MIN_SCORE
                            // to differentiate it from filtered tailQuematches that would also produce a 0 score.
                            float docScore = docs[upTo].score;
                            return docScore == 0 ? DEFAULT_MIN_SCORE : docScore;
                        }

                        @Override
                        public float getMaxScore(int upTo) throws IOException {
                            return Float.POSITIVE_INFINITY;
                        }

                        @Override
                        public int docID() {
                            return upTo < start ? -1 : docs[upTo].doc;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            for (Query source : sources) {
                source.visit(visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this));
            }
        }

        @Override
        public String toString(String field) {
            StringBuilder sb = new StringBuilder("rank_top(");
            for (int i = 0; i < sources.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                if (queryNames[i] != null) {
                    sb.append(queryNames[i]).append("=");
                }
                sb.append(sources[i]);
            }
            return sb.append(")").toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(docs), Arrays.hashCode(sources), Arrays.hashCode(queryNames), contextIdentity);
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj) == false) {
                return false;
            }
            TopQuery other = (TopQuery) obj;
            return Arrays.equals(docs, other.docs)
                && Arrays.equals(sources, other.sources)
                && Arrays.equals(queryNames, other.queryNames)
                && Objects.equals(contextIdentity, other.contextIdentity);
        }
    }

    /**
     * Creates a {@code RankDocsQuery} based on the provided docs.
     * @param reader       The index reader
     * @param rankDocs     The docs to rank
     * @param sources      The original queries that were used to compute the top documents
     * @param queryNames   The names (if present) of the original retrievers
     * @param onlyRankDocs Whether the query should only match the provided rank docs
     * @param minScore     The minimum score threshold for documents to be included in total hits.
     *                     This can be set to any value including 0.0f to filter out documents with scores below the threshold.
     *                     Note: This is separate from the special handling of 0 scores. Documents with a score of exactly 0
     *                     will always be assigned DEFAULT_MIN_SCORE internally to differentiate them from filtered matches,
     *                     regardless of the minScore value.
     */
    public RankDocsQuery(
        IndexReader reader,
        RankDoc[] rankDocs,
        Query[] sources,
        String[] queryNames,
        boolean onlyRankDocs,
        float minScore
    ) {
        assert sources.length == queryNames.length;
        // clone to avoid side-effect after sorting
        this.docs = rankDocs.clone();
        // sort rank docs by doc id
        Arrays.sort(docs, Comparator.comparingInt(a -> a.doc));
        this.topQuery = new TopQuery(docs, sources, queryNames, findSegmentStarts(reader, docs), reader.getContext().id(), minScore);
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
        this.minScore = minScore;
    }

    private RankDocsQuery(RankDoc[] docs, Query topQuery, Query tailQuery, boolean onlyRankDocs, float minScore) {
        this.docs = docs;
        this.topQuery = topQuery;
        this.tailQuery = tailQuery;
        this.onlyRankDocs = onlyRankDocs;
        this.minScore = minScore;
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
                return new ScorerSupplier() {
                    private final ScorerSupplier supplier = combinedWeight.scorerSupplier(context);

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        Scorer scorer = supplier.get(leadCost);
                        if (minScore > DEFAULT_MIN_SCORE) {
                            return new MinScoreScorer(scorer, minScore);
                        }
                        return scorer;
                    }

                    @Override
                    public long cost() {
                        return supplier.cost();
                    }
                };
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        topQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this));
        if (tailQuery != null) {
            tailQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.FILTER, this));
        }
    }

    @Override
    public String toString(String field) {
        return "rank_docs(" + topQuery + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        RankDocsQuery other = (RankDocsQuery) obj;
        return Arrays.equals(docs, other.docs)
            && Objects.equals(topQuery, other.topQuery)
            && Objects.equals(tailQuery, other.tailQuery)
            && onlyRankDocs == other.onlyRankDocs
            && minScore == other.minScore;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(docs), topQuery, tailQuery, onlyRankDocs, minScore);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query topRewrite = searcher.rewrite(topQuery);
        boolean hasChanged = topRewrite != topQuery;
        Query tailRewrite = tailQuery;
        if (tailQuery != null) {
            tailRewrite = searcher.rewrite(tailQuery);
        }
        if (tailRewrite != tailQuery) {
            hasChanged = true;
        }
        return hasChanged ? new RankDocsQuery(docs, topRewrite, tailRewrite, onlyRankDocs, minScore) : this;
    }
} 