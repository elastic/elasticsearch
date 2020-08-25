/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.AbstractLongScriptFieldScript;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public final class LongScriptFieldDistanceFeatureQuery extends AbstractScriptFieldQuery {
    private final CheckedFunction<LeafReaderContext, AbstractLongScriptFieldScript, IOException> leafFactory;
    private final long origin;
    private final long pivot;
    private final float boost;

    public LongScriptFieldDistanceFeatureQuery(
        Script script,
        CheckedFunction<LeafReaderContext, AbstractLongScriptFieldScript, IOException> leafFactory,
        String fieldName,
        long origin,
        long pivot,
        float boost
    ) {
        super(script, fieldName);
        this.leafFactory = leafFactory;
        this.origin = origin;
        this.pivot = pivot;
        this.boost = boost;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public void extractTerms(Set<Term> terms) {}

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return new DistanceScorer(this, leafFactory.apply(context), context.reader().maxDoc(), boost);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                AbstractLongScriptFieldScript script = leafFactory.apply(context);
                long value = valueWithMinAbsoluteDistance(script.runForDoc(doc));
                float weight = LongScriptFieldDistanceFeatureQuery.this.boost * boost;
                float score = score(weight, distanceFor(value));
                return Explanation.match(
                    score,
                    "Distance score, computed as weight * pivot / (pivot + abs(value - origin)) from:",
                    Explanation.match(weight, "weight"),
                    Explanation.match(pivot, "pivot"),
                    Explanation.match(origin, "origin"),
                    Explanation.match(value, "current value")
                );
            }
        };
    }

    private class DistanceScorer extends Scorer {
        private final TwoPhaseIterator twoPhase;
        private final DocIdSetIterator disi;
        private final float weight;
        private long[] values;

        protected DistanceScorer(Weight weight, AbstractLongScriptFieldScript script, int maxDoc, float boost) {
            super(weight);
            twoPhase = new TwoPhaseIterator(DocIdSetIterator.all(maxDoc)) {
                @Override
                public boolean matches() throws IOException {
                    values = script.runForDoc(approximation().docID());
                    return values.length > 0;
                }

                @Override
                public float matchCost() {
                    return MATCH_COST;
                }
            };
            disi = TwoPhaseIterator.asDocIdSetIterator(twoPhase);
            this.weight = LongScriptFieldDistanceFeatureQuery.this.boost * boost;
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return disi;
        }

        @Override
        public TwoPhaseIterator twoPhaseIterator() {
            return twoPhase;
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return weight;
        }

        @Override
        public float score() throws IOException {
            if (values.length == 0) {
                return 0;
            }
            return LongScriptFieldDistanceFeatureQuery.this.score(weight, (double) minAbsoluteDistance(values));
        }
    }

    long minAbsoluteDistance(long[] values) {
        long minDistance = Long.MAX_VALUE;
        for (long v : values) {
            minDistance = Math.min(minDistance, distanceFor(v));
        }
        return minDistance;
    }

    long valueWithMinAbsoluteDistance(long[] values) {
        long minDistance = Long.MAX_VALUE;
        long minDistanceValue = Long.MAX_VALUE;
        for (long v : values) {
            long distance = distanceFor(v);
            if (distance < minDistance) {
                minDistance = distance;
                minDistanceValue = v;
            }
        }
        return minDistanceValue;
    }

    long distanceFor(long value) {
        long distance = Math.max(value, origin) - Math.min(value, origin);
        if (distance < 0) {
            // The distance doesn't fit into signed long so clamp it to MAX_VALUE
            return Long.MAX_VALUE;
        }
        return distance;
    }

    float score(float weight, double distance) {
        return (float) (weight * (pivot / (pivot + distance)));
    }

    @Override
    public String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (false == fieldName().equals(field)) {
            b.append(fieldName()).append(":");
        }
        b.append(getClass().getSimpleName());
        b.append("(origin=").append(origin);
        b.append(",pivot=").append(pivot);
        b.append(",boost=").append(boost).append(")");
        return b.toString();

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), origin, pivot, boost);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        LongScriptFieldDistanceFeatureQuery other = (LongScriptFieldDistanceFeatureQuery) obj;
        return origin == other.origin && pivot == other.pivot && boost == other.boost;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        // No subclasses contain any Terms because those have to be strings.
        if (visitor.acceptField(fieldName())) {
            visitor.visitLeaf(this);
        }
    }

    long origin() {
        return origin;
    }

    long pivot() {
        return pivot;
    }

    float boost() {
        return boost;
    }
}
