/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.script.ScoreScriptUtils;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.Decay.DecayFunction;

import java.util.function.DoubleUnaryOperator;
import java.util.function.ToDoubleFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Scorers for {@link Decay}, built once per query from its constant arguments, so scoring a row does no parsing and
 * allocates no {@link ScoreScriptUtils} objects. Each scorer is immutable and shared by every driver running the query.
 */
final class DecayScorers {

    private DecayScorers() {}

    /**
     * Scores numeric values with the same {@link ScoreScriptUtils} classes Painless uses.
     */
    static final class NumericScorer {
        private final DoubleUnaryOperator decay;
        private final String description;

        private NumericScorer(DoubleUnaryOperator decay, String description) {
            this.decay = decay;
            this.description = description;
        }

        static NumericScorer of(DecayFunction function, double origin, double scale, double offset, double decay) {
            DoubleUnaryOperator score = switch (function) {
                case LINEAR -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay)::decayNumericLinear;
                case EXPONENTIAL -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay)::decayNumericExp;
                case GAUSSIAN -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay)::decayNumericGauss;
            };
            return new NumericScorer(score, describe("NumericScorer", function, origin, scale, offset, decay));
        }

        double score(double value) {
            return decay.applyAsDouble(value);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * Scores {@code geo_point} values by arc distance with the same {@link ScoreScriptUtils} classes Painless uses.
     */
    static final class GeoScorer {
        private final ToDoubleFunction<GeoPoint> decay;
        private final String description;

        private GeoScorer(ToDoubleFunction<GeoPoint> decay, String description) {
            this.decay = decay;
            this.description = description;
        }

        static GeoScorer of(DecayFunction function, BytesRef origin, String scale, String offset, double decay) {
            Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);
            // GeoUtils.parseGeoPoint expects coordinate strings in "lat,lon" order; use getY (lat) then getX (lon).
            String originString = originPoint.getY() + "," + originPoint.getX();
            ToDoubleFunction<GeoPoint> score = switch (function) {
                case LINEAR -> new ScoreScriptUtils.DecayGeoLinear(originString, scale, offset, decay)::decayGeoLinear;
                case EXPONENTIAL -> new ScoreScriptUtils.DecayGeoExp(originString, scale, offset, decay)::decayGeoExp;
                case GAUSSIAN -> new ScoreScriptUtils.DecayGeoGauss(originString, scale, offset, decay)::decayGeoGauss;
            };
            return new GeoScorer(score, describe("GeoScorer", function, originString, scale, offset, decay));
        }

        double score(BytesRef value) {
            Point point = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
            return decay.applyAsDouble(new GeoPoint(point.getY(), point.getX()));
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * Scores {@code cartesian_point} values by euclidean distance.
     */
    static final class CartesianScorer {
        private final double originX;
        private final double originY;
        private final double offset;
        private final DoubleUnaryOperator decayOfDistance;
        private final String description;

        private CartesianScorer(double originX, double originY, double offset, DoubleUnaryOperator decayOfDistance, String description) {
            this.originX = originX;
            this.originY = originY;
            this.offset = offset;
            this.decayOfDistance = decayOfDistance;
            this.description = description;
        }

        static CartesianScorer of(DecayFunction function, BytesRef origin, double scale, double offset, double decay) {
            Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);
            DoubleUnaryOperator decayOfDistance = switch (function) {
                case LINEAR -> {
                    double scaling = scale / (1.0 - decay);
                    yield distance -> Math.max(0.0, (scaling - distance) / scaling);
                }
                case EXPONENTIAL -> {
                    double scaling = Math.log(decay) / scale;
                    yield distance -> Math.exp(scaling * distance);
                }
                case GAUSSIAN -> {
                    double sigmaSquared = -Math.pow(scale, 2.0) / (2.0 * Math.log(decay));
                    yield distance -> Math.exp(-Math.pow(distance, 2.0) / (2.0 * sigmaSquared));
                }
            };
            return new CartesianScorer(
                originPoint.getX(),
                originPoint.getY(),
                offset,
                decayOfDistance,
                describe("CartesianScorer", function, originPoint, scale, offset, decay)
            );
        }

        double score(BytesRef value) {
            Point point = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
            double dx = point.getX() - originX;
            double dy = point.getY() - originY;
            return decayOfDistance.applyAsDouble(Math.max(0.0, Math.sqrt(dx * dx + dy * dy) - offset));
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * Scores {@code datetime} and {@code date_nanos} values. The origin, scale and offset are in the value's unit,
     * milliseconds or nanoseconds. The distance is computed in {@code long} and then scored like a number.
     */
    static final class TemporalScorer {
        private final long origin;
        private final long offset;
        private final NumericScorer decayOfDistance;
        private final String description;

        private TemporalScorer(long origin, long offset, NumericScorer decayOfDistance, String description) {
            this.origin = origin;
            this.offset = offset;
            this.decayOfDistance = decayOfDistance;
            this.description = description;
        }

        static TemporalScorer of(DecayFunction function, long origin, long scale, long offset, double decay) {
            return new TemporalScorer(
                origin,
                offset,
                NumericScorer.of(function, 0.0, scale, 0.0, decay),
                describe("TemporalScorer", function, origin, scale, offset, decay)
            );
        }

        double score(long value) {
            // as java.lang.Math#abs(long) is a forbidden API, have to use this comparison instead
            long diff = (value >= origin) ? (value - origin) : (origin - value);
            if (diff >= 0) {
                return decayOfDistance.score(Math.max(0, diff - offset));
            }
            // the subtraction overflowed, so diff holds the exact distance as an unsigned long above Long.MAX_VALUE
            return decayOfDistance.score(NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(diff)) - offset);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private static String describe(String name, DecayFunction function, Object origin, Object scale, Object offset, double decay) {
        return format(null, "{}[type={}, origin={}, scale={}, offset={}, decay={}]", name, function, origin, scale, offset, decay);
    }
}
