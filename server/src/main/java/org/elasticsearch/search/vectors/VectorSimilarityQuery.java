/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.search.function.MinScoreScorer;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.Strings.format;

/**
 * This query provides a simple post-filter for the provided Query. The query is assumed to be a Knn(Float|Byte)VectorQuery.
 */
public class VectorSimilarityQuery extends Query {
    private final float similarity;
    private final float docScore;
    private final Query innerKnnQuery;

    /**
     * @param innerKnnQuery A {@link org.apache.lucene.search.KnnFloatVectorQuery} or {@link org.apache.lucene.search.KnnByteVectorQuery}
     * @param similarity The similarity threshold originally provided (used in explanations)
     * @param docScore The similarity transformed into a score threshold applied after gathering the nearest neighbors
     */
    public VectorSimilarityQuery(Query innerKnnQuery, float similarity, float docScore) {
        this.similarity = similarity;
        this.docScore = docScore;
        this.innerKnnQuery = innerKnnQuery;
    }

    // For testing
    Query getInnerKnnQuery() {
        return innerKnnQuery;
    }

    float getSimilarity() {
        return similarity;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewrittenInnerQuery = innerKnnQuery.rewrite(searcher);
        if (rewrittenInnerQuery instanceof MatchNoDocsQuery) {
            return rewrittenInnerQuery;
        }
        if (rewrittenInnerQuery == innerKnnQuery) {
            return this;
        }
        return new VectorSimilarityQuery(rewrittenInnerQuery, similarity, docScore);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight innerWeight;
        if (scoreMode.isExhaustive()) {
            innerWeight = innerKnnQuery.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        } else {
            innerWeight = innerKnnQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);
        }
        return new MinScoreWeight(innerWeight, docScore, similarity, this, boost);
    }

    @Override
    public String toString(String field) {
        return "VectorSimilarityQuery["
            + "similarity="
            + similarity
            + ", docScore="
            + docScore
            + ", innerKnnQuery="
            + innerKnnQuery.toString(field)
            + ']';
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
        VectorSimilarityQuery other = (VectorSimilarityQuery) obj;
        return Objects.equals(innerKnnQuery, other.innerKnnQuery) && docScore == other.docScore && similarity == other.similarity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerKnnQuery, docScore, similarity);
    }

    private static class MinScoreWeight extends FilterWeight {

        private final float similarity, docScore, boost;

        private MinScoreWeight(Weight innerWeight, float docScore, float similarity, Query parent, float boost) {
            super(parent, innerWeight);
            this.docScore = docScore;
            this.similarity = similarity;
            this.boost = boost;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation explanation = in.explain(context, doc);
            if (explanation.isMatch()) {
                float score = explanation.getValue().floatValue();
                if (score >= docScore) {
                    return Explanation.match(explanation.getValue().floatValue() * boost, "vector similarity within limit", explanation);
                } else {
                    return Explanation.noMatch(
                        format(
                            "vector found, but score [%f] is less than matching minimum score [%f] from similarity [%f]",
                            explanation.getValue().floatValue(),
                            docScore,
                            similarity
                        ),
                        explanation
                    );
                }
            }
            return explanation;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            ScorerSupplier inScorerSupplier = in.scorerSupplier(context);
            if (inScorerSupplier == null) {
                return null;
            }
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) throws IOException {
                    return new MinScoreScorer(inScorerSupplier.get(leadCost), docScore, boost);
                }

                @Override
                public long cost() {
                    return inScorerSupplier.cost();
                }
            };
        }
    }

}
