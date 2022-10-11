/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.FunctionalBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ProximityBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ValueBoost;

public class ScriptScoreBoostsTests extends ESTestCase {
    public void testValueSource() {
        ValueBoost val1 = new ValueBoost("5", "add", 2f);
        String actualVal1 = val1.getSource("foo");
        assertEquals("(((doc['foo'].size() > 0) && (doc['foo'].value.toString() == '5')) ? 2 : 0)", actualVal1);

        ValueBoost val2 = new ValueBoost("5", "multiply", 2f);
        String actualVal2 = val2.getSource("foo");
        assertEquals("(((doc['foo'].size() > 0) && (doc['foo'].value.toString() == '5')) ? 2 : 1)", actualVal2);
    }

    public void testFunctionalSource() {
        FunctionalBoost linear1 = new FunctionalBoost("linear", "add", 3f);
        String actualLin1 = linear1.getSource("foo");
        assertEquals("3 * ((doc['foo'].size() > 0) ? doc['foo'].value : 0)", actualLin1);

        FunctionalBoost linear2 = new FunctionalBoost("linear", "multiply", 3f);
        String actualLin2 = linear2.getSource("foo");
        assertEquals("3 * ((doc['foo'].size() > 0) ? doc['foo'].value : 1)", actualLin2);

        FunctionalBoost exp1 = new FunctionalBoost("exponential", "add", 3f);
        String actualExp1 = exp1.getSource("foo");
        assertEquals("3 * Math.exp((doc['foo'].size() > 0) ? doc['foo'].value : 0)", actualExp1);

        FunctionalBoost exp2 = new FunctionalBoost("exponential", "multiply", 3f);
        String actualExp2 = exp2.getSource("foo");
        assertEquals("3 * Math.exp((doc['foo'].size() > 0) ? doc['foo'].value : 1)", actualExp2);

        FunctionalBoost log1 = new FunctionalBoost("logarithmic", "add", 3f);
        String actualLog1 = log1.getSource("foo");
        assertEquals("3 * Math.max(0.0001, Math.log(Math.max(0.0001, (doc['foo'].size() > 0) ? (doc['foo'].value + 1) : 0)))", actualLog1);

        FunctionalBoost log2 = new FunctionalBoost("logarithmic", "multiply", 3f);
        String actualLog2 = log2.getSource("foo");
        assertEquals("3 * Math.max(0.0001, Math.log(Math.max(0.0001, (doc['foo'].size() > 0) ? (doc['foo'].value + 1) : 1)))", actualLog2);
    }

    public void testProximitySource() {
        // location
        ProximityBoost boost = new ProximityBoost("25.32, -80.93", "linear", 8f);
        assertEquals(
            "8 * ((doc['foo'].size() > 0) ? decayGeoLinear('25.32, -80.93', '1km', '0km', 0.5, doc['foo'].value) : 0)",
            boost.getSource("foo")
        );

        boost = new ProximityBoost("25.32, -80.93", "exponential", 8f);
        assertEquals(
            "8 * ((doc['foo'].size() > 0) ? decayGeoExp('25.32, -80.93', '1km', '0km', 0.5, doc['foo'].value) : 0)",
            boost.getSource("foo")
        );

        boost = new ProximityBoost("25.32, -80.93", "gaussian", 8f);
        assertEquals(
            "8 * ((doc['foo'].size() > 0) ? decayGeoGauss('25.32, -80.93', '1km', '0km', 0.5, doc['foo'].value) : 0)",
            boost.getSource("foo")
        );

        // date
        boost = new ProximityBoost("2022-01-01", "linear", 8f);
        assertEquals(
            "8 * ((doc['foo'].size() > 0) ? decayDateLinear('2022-01-01', '1d', '0', 0.5, doc['foo'].value) : 0)",
            boost.getSource("foo")
        );

        boost = new ProximityBoost("2022-01-01", "exponential", 8f);
        assertEquals(
            "8 * ((doc['foo'].size() > 0) ? decayDateExp('2022-01-01', '1d', '0', 0.5, doc['foo'].value) : 0)",
            boost.getSource("foo")
        );

        boost = new ProximityBoost("2022-01-01", "gaussian", 8f);
        assertEquals(
            "8 * ((doc['foo'].size() > 0) ? decayDateGauss('2022-01-01', '1d', '0', 0.5, doc['foo'].value) : 0)",
            boost.getSource("foo")
        );

        boost = new ProximityBoost("now", "linear", 8f);
        assertNotEquals(boost.getDateCenter(), "now");
        assertEquals(
            "8 * ((doc['foo'].size() > 0) ? decayDateLinear('" + boost.getDateCenter() + "', '1d', '0', 0.5, doc['foo'].value) : 0)",
            boost.getSource("foo")
        );

        // numeric
        boost = new ProximityBoost("20", "linear", 8f);
        assertEquals("8 * ((doc['foo'].size() > 0) ? decayNumericLinear(20, 10, 0.0, 0.5, doc['foo'].value) : 0)", boost.getSource("foo"));

        boost = new ProximityBoost("20", "exponential", 8f);
        assertEquals("8 * ((doc['foo'].size() > 0) ? decayNumericExp(20, 10, 0.0, 0.5, doc['foo'].value) : 0)", boost.getSource("foo"));

        boost = new ProximityBoost("20", "gaussian", 8f);
        assertEquals("8 * ((doc['foo'].size() > 0) ? decayNumericGauss(20, 10, 0.0, 0.5, doc['foo'].value) : 0)", boost.getSource("foo"));
    }

    public void testIsGeo() {
        assertEquals(true, new ProximityBoost("40, -70.12", "linear", 2f).isGeo());
        assertEquals(true, new ProximityBoost("drm3btev3e86", "linear", 2f).isGeo());
        assertEquals(true, new ProximityBoost("POINT (-71.34 41.12)", "linear", 2f).isGeo());
        assertEquals(false, new ProximityBoost("other", "linear", 2f).isGeo());
    }

    public void testIsNumber() {
        assertEquals(true, new ProximityBoost("1", "linear", 2f).isNumber());
        assertEquals(true, new ProximityBoost("1.5", "linear", 2f).isNumber());
        assertEquals(true, new ProximityBoost("2.5e-6", "linear", 2f).isNumber());
        assertEquals(false, new ProximityBoost("nothing", "linear", 2f).isNumber());
    }

    public void testIsDate() {
        assertEquals(true, new ProximityBoost("now", "linear", 2f).isDate());
        assertEquals(false, new ProximityBoost("blah", "linear", 2f).isDate());
        assertEquals(true, new ProximityBoost("2022-01-02", "linear", 2f).isDate());
    }
}
