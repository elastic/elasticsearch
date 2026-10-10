/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ScaleRegulatorTests extends ESTestCase {

    private final ScaleRegulator regulator = new ScaleRegulator();

    private void pick(int picks) {
        for (int i = 0; i < picks; i++) {
            regulator.recordPick();
        }
    }

    public void testNothingChangesWithoutATarget() {
        pick(1000);
        regulator.observe(3600);

        assertThat(regulator.adjustment(), equalTo(1.0));
    }

    public void testWaitsForAWindowBeforeLookingAtTheRate() {
        regulator.targetPerHour(3600); // a window is 10 picks, which is 10 seconds, so the minimum of 30 seconds
        pick(1000);

        regulator.observe(10);
        regulator.observe(10);
        assertThat("less than a window", regulator.adjustment(), equalTo(1.0));

        regulator.observe(10);
        assertThat("a window", regulator.adjustment(), equalTo(0.5));
    }

    public void testPicksTooFastBringsTheScaleDown() {
        regulator.targetPerHour(3600);
        pick(60); // 7200 an hour over 30 seconds, twice the target

        regulator.observe(30);

        assertThat(regulator.adjustment(), closeTo(Math.sqrt(0.5), 1e-12));
    }

    public void testPicksTooSlowlyRaisesTheScale() {
        regulator.targetPerHour(3600);
        pick(10); // 1200 an hour over 30 seconds, a third of the target

        regulator.observe(30);

        assertThat(regulator.adjustment(), closeTo(Math.sqrt(3.0), 1e-12));
    }

    public void testASingleStepIsNeverMoreThanAFactorOfTwo() {
        regulator.targetPerHour(3600);
        regulator.observe(30);
        assertThat("no picks at all", regulator.adjustment(), equalTo(2.0));

        pick(100_000);
        regulator.observe(30);
        assertThat("far too many", regulator.adjustment(), equalTo(1.0));
    }

    public void testTheAdjustmentIsBounded() {
        regulator.targetPerHour(3600);
        for (int i = 0; i < 100; i++) {
            regulator.observe(30);
        }
        assertThat(regulator.adjustment(), equalTo(ScaleRegulator.MAX_ADJUSTMENT));

        pick(10_000_000);
        for (int i = 0; i < 100; i++) {
            regulator.observe(30);
            pick(10_000_000);
        }
        assertThat(regulator.adjustment(), equalTo(ScaleRegulator.MIN_ADJUSTMENT));
    }

    public void testASmallTargetNeedsALongerWindow() {
        regulator.targetPerHour(10); // 10 picks take an hour
        regulator.observe(1800);
        assertThat(regulator.adjustment(), equalTo(1.0));

        regulator.observe(1800);
        assertThat(regulator.adjustment(), equalTo(2.0));
    }

    public void testSettlesAtATargetAndStaysThere() {
        regulator.targetPerHour(3600);
        // each window picks as many as the target asks for at the adjustment of that moment, whatever it is
        double picksPerWindowAtFullScale = 120; // the picks per 30 seconds that the traffic gives with an adjustment of 1
        for (int window = 0; window < 20; window++) {
            pick((int) Math.round(picksPerWindowAtFullScale * regulator.adjustment()));
            regulator.observe(30);
        }

        assertThat("30 seconds of 3600 picks an hour", picksPerWindowAtFullScale * regulator.adjustment(), closeTo(30, 1.5));
    }

    public void testRemovingTheTargetPutsTheScaleBack() {
        regulator.targetPerHour(3600);
        regulator.observe(30);
        assertThat(regulator.adjustment(), equalTo(2.0));

        regulator.targetPerHour(0);
        regulator.observe(1);

        assertThat(regulator.adjustment(), equalTo(1.0));
    }
}
