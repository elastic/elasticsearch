/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;
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
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public final class GeoPointScriptFieldDistanceFeatureQuery extends AbstractScriptFieldQuery<AbstractLongFieldScript> {
    private final double originLat;
    private final double originLon;
    private final double pivotDistance;

    public GeoPointScriptFieldDistanceFeatureQuery(
        Script script,
        Function<LeafReaderContext, AbstractLongFieldScript> leafFactory,
        String fieldName,
        double originLat,
        double originLon,
        double pivotDistance
    ) {
        super(script, fieldName, leafFactory);
        GeoUtils.checkLatitude(originLat);
        GeoUtils.checkLongitude(originLon);
        this.originLon = originLon;
        this.originLat = originLat;
        if (pivotDistance <= 0) {
            throw new IllegalArgumentException("pivotDistance must be > 0, got " + pivotDistance);
        }
        this.pivotDistance = pivotDistance;
    }

    double lat() {
        return originLat;
    }

    double lon() {
        return originLon;
    }

    double pivot() {
        return pivotDistance;
    }

    @Override
    protected boolean matches(AbstractLongFieldScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return scriptContext.count() > 0;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new Weight(this) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                Scorer scorer = new DistanceScorer(scriptContextFunction().apply(context), context.reader().maxDoc(), boost);
                return new DefaultScorerSupplier(scorer);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                AbstractLongFieldScript script = scriptContextFunction().apply(context);
                script.runForDoc(doc);
                long encoded = valueWithMinAbsoluteDistance(script);
                int latitudeBits = (int) (encoded >> 32);
                int longitudeBits = (int) (encoded & 0xFFFFFFFF);
                double lat = GeoEncodingUtils.decodeLatitude(latitudeBits);
                double lon = GeoEncodingUtils.decodeLongitude(longitudeBits);
                double distance = SloppyMath.haversinMeters(originLat, originLon, lat, lon);
                float score = (float) (boost * (pivotDistance / (pivotDistance + distance)));
                return Explanation.match(
                    score,
                    "Distance score, computed as weight * pivotDistance / (pivotDistance + abs(distance)) from:",
                    Explanation.match(boost, "weight"),
                    Explanation.match(pivotDistance, "pivotDistance"),
                    Explanation.match(originLat, "originLat"),
                    Explanation.match(originLon, "originLon"),
                    Explanation.match(lat, "current lat"),
                    Explanation.match(lon, "current lon"),
                    Explanation.match(distance, "distance")
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
                    return GeoPointScriptFieldDistanceFeatureQuery.this.matches(script, approximation.docID());
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
        public float score() throws IOException {
            if (script.count() == 0) {
                return 0;
            }
            return GeoPointScriptFieldDistanceFeatureQuery.this.score(weight, getDistance(script));
        }

        @Override
        public float getMaxScore(int upTo) {
            return weight;
        }

        @Override
        public DocIdSetIterator iterator() {
            return disi;
        }

        @Override
        public TwoPhaseIterator twoPhaseIterator() {
            return twoPhase;
        }
    }

    private double getDistance(AbstractLongFieldScript script) {
        double minDistance = Double.POSITIVE_INFINITY;
        for (int i = 0; i < script.count(); i++) {
            minDistance = Math.min(minDistance, getDistanceFromEncoded(script.values()[i]));
        }
        return minDistance;
    }

    private double getDistanceFromEncoded(long encoded) {
        int latitudeBits = (int) (encoded >> 32);
        int longitudeBits = (int) (encoded & 0xFFFFFFFF);
        double lat = GeoEncodingUtils.decodeLatitude(latitudeBits);
        double lon = GeoEncodingUtils.decodeLongitude(longitudeBits);
        return SloppyMath.haversinMeters(originLat, originLon, lat, lon);
    }

    long valueWithMinAbsoluteDistance(AbstractLongFieldScript script) {
        double minDistance = Double.POSITIVE_INFINITY;
        long minDistanceValue = Long.MAX_VALUE;
        for (int i = 0; i < script.count(); i++) {
            double distance = getDistanceFromEncoded(script.values()[i]);
            if (distance < minDistance) {
                minDistance = distance;
                minDistanceValue = script.values()[i];
            }
        }
        return minDistanceValue;
    }

    float score(float weight, double distance) {
        return (float) (weight * (pivotDistance / (pivotDistance + distance)));
    }

    @Override
    public String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (false == fieldName().equals(field)) {
            b.append(fieldName()).append(":");
        }
        b.append(getClass().getSimpleName());
        b.append("(lat=").append(originLat);
        b.append(",lon=").append(originLon);
        b.append(",pivot=").append(pivotDistance).append(")");
        return b.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originLat, originLon, pivotDistance);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        GeoPointScriptFieldDistanceFeatureQuery other = (GeoPointScriptFieldDistanceFeatureQuery) obj;
        return originLon == other.originLon && originLat == other.originLat && pivotDistance == other.pivotDistance;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName())) {
            visitor.visitLeaf(this);
        }
    }
}
