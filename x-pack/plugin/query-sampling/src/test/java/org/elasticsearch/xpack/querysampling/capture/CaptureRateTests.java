/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class CaptureRateTests extends ESTestCase {

    public void testWithoutAFloorItIsTheConfiguredRate() {
        CaptureRate rate = new CaptureRate(0.2, 0);

        rate.observe(5, 1);
        assertThat(rate.effective(), equalTo(0.2));
        rate.observe(0, 1);
        assertThat("however quiet", rate.effective(), equalTo(0.2));
    }

    public void testWhatTheTrafficIsIsNotKnownUntilItWasObserved() {
        CaptureRate rate = new CaptureRate(0.01, 1000);

        assertThat("nothing is assumed about it", rate.effective(), equalTo(0.01));
    }

    public void testQuietTrafficGetsARateThatMeetsTheFloor() {
        CaptureRate rate = new CaptureRate(0.01, 360);

        rate.observe(1, 1); // one a second is 3600 an hour, so a tenth of them are the 360 asked for

        assertThat(rate.effective(), closeTo(0.1, 1e-12));
    }

    public void testTheConfiguredRateIsKeptWhenItAlreadyMeetsTheFloor() {
        CaptureRate rate = new CaptureRate(0.5, 360);

        rate.observe(1, 1);

        assertThat(rate.effective(), equalTo(0.5));
    }

    public void testTheRateNeverGoesAboveOne() {
        CaptureRate rate = new CaptureRate(0.01, 1_000_000);

        rate.observe(1, 1);
        assertThat(rate.effective(), equalTo(1.0));
        rate.observe(0, 1);
        assertThat("and no traffic means anything that comes is captured", rate.effective(), equalTo(1.0));
    }

    public void testTheArrivalRateIsSmoothed() {
        CaptureRate rate = new CaptureRate(0.0, 3600);

        rate.observe(100, 1);
        assertThat("the first observation is all there is", rate.effective(), closeTo(3600 / (100.0 * 3600), 1e-12));
        rate.observe(0, 1);
        // 30% of the new observation and 70% of what was known: 70 a second
        assertThat(rate.effective(), closeTo(3600 / (70.0 * 3600), 1e-12));
    }

    public void testObservationsAreOfAnyLength() {
        CaptureRate rate = new CaptureRate(0.0, 3600);

        rate.observe(50, 0.5);

        assertThat("50 in half a second is 100 a second", rate.effective(), closeTo(0.01, 1e-12));
    }

    public void testSettingsTakeEffectAtOnce() {
        CaptureRate rate = new CaptureRate(0.01, 0);
        rate.observe(1, 1);
        assertThat(rate.effective(), equalTo(0.01));

        rate.minPerHour(360);
        assertThat(rate.effective(), closeTo(0.1, 1e-12));

        rate.configured(0.3);
        assertThat(rate.effective(), equalTo(0.3));

        rate.minPerHour(0);
        rate.configured(0.02);
        assertThat(rate.effective(), equalTo(0.02));
    }

    public void testForgettingTheTrafficGoesBackToTheConfiguredRate() {
        CaptureRate rate = new CaptureRate(0.01, 360);
        rate.observe(1, 1);
        assertThat(rate.effective(), closeTo(0.1, 1e-12));

        rate.forgetTraffic();

        assertThat(rate.effective(), equalTo(0.01));
    }
}
