/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.Decay.DecayFunction;

/**
 * Scorers for {@link Decay}, built once per query from its constant arguments, so scoring a row does no parsing.
 * Each scorer holds only primitives, so it is immutable and shared by every driver running the query.
 * The {@code score} methods must not throw: the {@link Decay} evaluators declare no {@code warnExceptions},
 * so an exception would fail the query instead of turning the row into {@code null} with a warning.
 */
final class DecayScorers {

    private DecayScorers() {}

    /**
     * Scores numeric values by their absolute difference from the origin.
     */
    record NumericScorer(DecayFunction function, double origin, double offset, double scaling) {
        static NumericScorer of(DecayFunction function, double origin, double scale, double offset, double decay) {
            return new NumericScorer(function, origin, offset, function.scaling(scale, decay));
        }

        double score(double value) {
            return function.score(scaling, Math.max(0.0, Math.abs(value - origin) - offset));
        }
    }

    /**
     * Scores {@code geo_point} values by arc distance in meters.
     */
    record GeoScorer(DecayFunction function, double originLat, double originLon, double offset, double scaling) {
        static GeoScorer of(DecayFunction function, BytesRef origin, double scale, double offset, double decay) {
            Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);
            return new GeoScorer(function, originPoint.getY(), originPoint.getX(), offset, function.scaling(scale, decay));
        }

        double score(BytesRef value) {
            Point point = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
            double distance = GeoDistance.ARC.calculate(originLat, originLon, point.getY(), point.getX(), DistanceUnit.METERS);
            return function.score(scaling, Math.max(0.0, distance - offset));
        }
    }

    /**
     * Scores {@code cartesian_point} values by euclidean distance.
     */
    record CartesianScorer(DecayFunction function, double originX, double originY, double offset, double scaling) {
        static CartesianScorer of(DecayFunction function, BytesRef origin, double scale, double offset, double decay) {
            Point originPoint = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(origin);
            return new CartesianScorer(function, originPoint.getX(), originPoint.getY(), offset, function.scaling(scale, decay));
        }

        double score(BytesRef value) {
            Point point = SpatialCoordinateTypes.UNSPECIFIED.wkbAsPoint(value);
            double dx = point.getX() - originX;
            double dy = point.getY() - originY;
            return function.score(scaling, Math.max(0.0, Math.sqrt(dx * dx + dy * dy) - offset));
        }
    }

    /**
     * Scores {@code datetime} and {@code date_nanos} values. The origin and offset are in the value's unit,
     * milliseconds or nanoseconds. The distance is computed in {@code long} and then scored like a number.
     */
    record TemporalScorer(DecayFunction function, long origin, long offset, double scaling) {
        static TemporalScorer of(DecayFunction function, long origin, long scale, long offset, double decay) {
            return new TemporalScorer(function, origin, offset, function.scaling(scale, decay));
        }

        double score(long value) {
            // as java.lang.Math#abs(long) is a forbidden API, have to use this comparison instead
            long diff = (value >= origin) ? (value - origin) : (origin - value);
            if (diff >= 0) {
                return function.score(scaling, Math.max(0, diff - offset));
            }
            // the subtraction overflowed, so diff holds the exact distance as an unsigned long above Long.MAX_VALUE;
            // offset is below it, so subtracting in unsigned arithmetic is exact
            return function.score(scaling, NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(diff - offset)));
        }
    }
}
