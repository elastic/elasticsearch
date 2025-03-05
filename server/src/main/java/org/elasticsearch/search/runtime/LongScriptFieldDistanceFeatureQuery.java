/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public final class LongScriptFieldDistanceFeatureQuery extends AbstractScriptFieldQuery<AbstractLongFieldScript> {
    private final long origin;
    private final long pivot;

    public LongScriptFieldDistanceFeatureQuery(
        Script script,
        Function<LeafReaderContext, AbstractLongFieldScript> leafFactory,
        String fieldName,
        long origin,
        long pivot
    ) {
        super(script, fieldName, leafFactory);
        this.origin = origin;
        this.pivot = pivot;
    }

    @Override
    protected boolean matches(AbstractLongFieldScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return scriptContext.count() > 0;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                return new DefaultScorerSupplier(
                    new DistanceScorer(scriptContextFunction().apply(context), context.reader().maxDoc(), boost)
                );
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                AbstractLongFieldScript script = scriptContextFunction().apply(context);
                script.runForDoc(doc);
                long value = valueWithMinAbsoluteDistance(script);
                float score = score(boost, distanceFor(value));
                return Explanation.match(
                    score,
                    "Distance score, computed as weight * pivot / (pivot + abs(value - origin)) from:",
                    Explanation.match(boost, "weight"),
                    Explanation.match(pivot, "pivot"),
                    Explanation.match(origin, "origin"),
                    Explanation.match(value, "current value")
                );
            }
        };
    }

    private class DistanceScorer extends Scorer {
        private final AbstractLongFieldScript script;
        private final TwoPhaseIterator twoPhase;
        private final DocIdSetIterator disi;
        private final float weight;

        protected DistanceScorer(AbstractLongFieldScript script, int maxDoc, float boost) {
            this.script = script;
            twoPhase = new TwoPhaseIterator(DocIdSetIterator.all(maxDoc)) {
                @Override
                public boolean matches() {
                    return LongScriptFieldDistanceFeatureQuery.this.matches(script, approximation.docID());
                }

                @Override
                public float matchCost() {
                    return MATCH_COST;
                }
            };
            disi = TwoPhaseIterator.asDocIdSetIterator(twoPhase);
            this.weight = boost;
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
        public float getMaxScore(int upTo) {
            return weight;
        }

        @Override
        public float score() {
            if (script.count() == 0) {
                return 0;
            }
            return LongScriptFieldDistanceFeatureQuery.this.score(weight, (double) minAbsoluteDistance(script));
        }
    }

    long minAbsoluteDistance(AbstractLongFieldScript script) {
        long minDistance = Long.MAX_VALUE;
        for (int i = 0; i < script.count(); i++) {
            minDistance = Math.min(minDistance, distanceFor(script.values()[i]));
        }
        return minDistance;
    }

    long valueWithMinAbsoluteDistance(AbstractLongFieldScript script) {
        long minDistance = Long.MAX_VALUE;
        long minDistanceValue = Long.MAX_VALUE;
        for (int i = 0; i < script.count(); i++) {
            long distance = distanceFor(script.values()[i]);
            if (distance < minDistance) {
                minDistance = distance;
                minDistanceValue = script.values()[i];
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
        b.append(",pivot=").append(pivot).append(")");
        return b.toString();

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), origin, pivot);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        LongScriptFieldDistanceFeatureQuery other = (LongScriptFieldDistanceFeatureQuery) obj;
        return origin == other.origin && pivot == other.pivot;
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
}
