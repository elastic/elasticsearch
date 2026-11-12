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
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.GeoScorer;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayScorers.NumericScorer;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link DecayScorers} copy the formulas of the {@link ScoreScriptUtils} decay functions, so they must return exactly the same scores.
 */
public class DecayScorersTests extends ESTestCase {

    public void testNumericMatchesScoreScriptUtils() {
        double origin = randomDoubleBetween(-1e6, 1e6, true);
        double scale = randomDoubleBetween(1e-3, 1e6, true);
        double offset = randomDoubleBetween(0, 1e3, true);
        double decay = randomDecay();
        double value = randomDoubleBetween(-1e6, 1e6, true);
        for (DecayFunction function : DecayFunction.values()) {
            double expected = switch (function) {
                case LINEAR -> new ScoreScriptUtils.DecayNumericLinear(origin, scale, offset, decay).decayNumericLinear(value);
                case EXPONENTIAL -> new ScoreScriptUtils.DecayNumericExp(origin, scale, offset, decay).decayNumericExp(value);
                case GAUSSIAN -> new ScoreScriptUtils.DecayNumericGauss(origin, scale, offset, decay).decayNumericGauss(value);
            };
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

    private static double randomDecay() {
        return randomDoubleBetween(1e-12, Math.nextDown(1.0), true);
    }

    private static BytesRef wkb(GeoPoint point) {
        return GEO.asWkb(new Point(point.lon(), point.lat()));
    }
}
