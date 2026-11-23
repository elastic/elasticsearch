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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.Decay.DecayFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.CartesianScorer;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.GeoScorer;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.NumericScorer;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.TemporalScorer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link DecayScorers} copy the formulas of the {@link ScoreScriptUtils} decay functions, so they must return exactly the same scores.
 * {@link ScoreScriptUtils} has no cartesian decay, so {@link CartesianScorer} is compared with the numeric decay of the distance.
 */
public class DecayScorersTests extends ESTestCase {

    public void testNumericMatchesScoreScriptUtils() {
        double origin = randomDoubleBetween(-1e6, 1e6, true);
        double scale = randomDoubleBetween(1e-3, 1e6, true);
        double offset = randomDoubleBetween(0, 1e3, true);
        double decay = randomDecay();
        double value = randomDoubleBetween(-1e6, 1e6, true);
        for (DecayFunction function : DecayFunction.values()) {
            double expected = numericDecay(function, origin, scale, offset, decay, value);
            assertThat(function.toString(), NumericScorer.of(function, origin, scale, offset, decay).score(value), equalTo(expected));
        }
    }

    public void testGeoMatchesScoreScriptUtils() {
        GeoPoint origin = RandomGeoGenerator.randomPoint(random());
        GeoPoint value = RandomGeoGenerator.randomPoint(random());
        double scale = randomDoubleBetween(1, 1e6, true);
        double offset = randomDoubleBetween(0, 1e5, true);
        double decay = randomDecay();
        String originString = origin.lat() + "," + origin.lon();
        String scaleString = scale + "m";
        String offsetString = offset + "m";
        for (DecayFunction function : DecayFunction.values()) {
            double expected = switch (function) {
                case LINEAR -> new ScoreScriptUtils.DecayGeoLinear(originString, scaleString, offsetString, decay).decayGeoLinear(value);
                case EXPONENTIAL -> new ScoreScriptUtils.DecayGeoExp(originString, scaleString, offsetString, decay).decayGeoExp(value);
                case GAUSSIAN -> new ScoreScriptUtils.DecayGeoGauss(originString, scaleString, offsetString, decay).decayGeoGauss(value);
            };
            assertThat(function.toString(), GeoScorer.of(function, wkb(origin), scale, offset, decay).score(wkb(value)), equalTo(expected));
        }
    }

    public void testCartesianMatchesScoreScriptUtils() {
        Point origin = randomCartesianPoint();
        Point value = randomCartesianPoint();
        double scale = randomDoubleBetween(1e-3, 1e6, true);
        double offset = randomDoubleBetween(0, 1e3, true);
        double decay = randomDecay();
        double dx = value.getX() - origin.getX();
        double dy = value.getY() - origin.getY();
        double distance = Math.sqrt(dx * dx + dy * dy);
        for (DecayFunction function : DecayFunction.values()) {
            double expected = numericDecay(function, 0, scale, offset, decay, distance);
            CartesianScorer scorer = CartesianScorer.of(function, CARTESIAN.asWkb(origin), scale, offset, decay);
            assertThat(function.toString(), scorer.score(CARTESIAN.asWkb(value)), equalTo(expected));
        }
    }

    public void testTemporalMatchesScoreScriptUtils() {
        long maxMillis = Instant.parse("2070-01-01T00:00:00Z").toEpochMilli();
        long origin = randomLongBetween(0, maxMillis);
        long value = randomLongBetween(0, maxMillis);
        long scale = randomLongBetween(1, maxMillis);
        long offset = randomLongBetween(0, Duration.ofDays(30).toMillis());
        double decay = randomDecay();
        String originString = Instant.ofEpochMilli(origin).toString();
        String scaleString = scale + "ms";
        String offsetString = offset + "ms";
        ZonedDateTime date = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
        for (DecayFunction function : DecayFunction.values()) {
            double expected = switch (function) {
                case LINEAR -> new ScoreScriptUtils.DecayDateLinear(originString, scaleString, offsetString, decay).decayDateLinear(date);
                case EXPONENTIAL -> new ScoreScriptUtils.DecayDateExp(originString, scaleString, offsetString, decay).decayDateExp(date);
                case GAUSSIAN -> new ScoreScriptUtils.DecayDateGauss(originString, scaleString, offsetString, decay).decayDateGauss(date);
            };
            assertThat(function.toString(), TemporalScorer.of(function, origin, scale, offset, decay).score(value), equalTo(expected));
        }
    }

    /**
     * The {@link ScoreScriptUtils} date decay overflows on dates more than {@link Long#MAX_VALUE} apart, so this compares with
     * the numeric decay of the exact distance instead.
     */
    public void testTemporalDistanceAboveLongMaxValue() {
        long scale = randomLongBetween(1, Long.MAX_VALUE);
        long offset = (1L << 62) - 1;
        double decay = randomDecay();
        // Long.MAX_VALUE - Long.MIN_VALUE - offset, which a double holds exactly
        double distance = 0x1p63 + 0x1p62;
        for (DecayFunction function : DecayFunction.values()) {
            double expected = numericDecay(function, 0, scale, 0, decay, distance);
            TemporalScorer scorer = TemporalScorer.of(function, Long.MIN_VALUE, scale, offset, decay);
            assertThat(function.toString(), scorer.score(Long.MAX_VALUE), equalTo(expected));
        }
    }

    private static double numericDecay(DecayFunction function, double origin, double scale, double offset, double decay, double value) {
        return switch (function) {
            case LINEAR -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
            case EXPONENTIAL -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
            case GAUSSIAN -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
        };
    }

    private static double randomDecay() {
        return randomDoubleBetween(1e-12, Math.nextDown(1.0), true);
    }

    private static Point randomCartesianPoint() {
        return new Point(randomDoubleBetween(-1e6, 1e6, true), randomDoubleBetween(-1e6, 1e6, true));
    }

    private static BytesRef wkb(GeoPoint point) {
        return GEO.asWkb(new Point(point.lon(), point.lat()));
    }
}
