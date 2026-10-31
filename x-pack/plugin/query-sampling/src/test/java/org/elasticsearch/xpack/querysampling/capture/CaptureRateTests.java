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

    public void testIsTheConfiguredRateUntilTheTrafficIsKnown() {
        CaptureRate rate = new CaptureRate(0.2, 1000);

        assertThat("no traffic seen yet, so no floor", rate.effective(), equalTo(0.2));
    }

    public void testTheFloorRaisesTheRateOfLowTraffic() {
        CaptureRate rate = new CaptureRate(0.01, 3600);

        rate.observe(10, 10); // 1 search a second: all of them are needed for 3600 an hour
        assertThat(rate.effective(), closeTo(1.0, 1e-12));

        rate = new CaptureRate(0.01, 3600);
        rate.observe(500, 10); // 50 a second: 2% give 3600 an hour
        assertThat(rate.effective(), closeTo(0.02, 1e-12));
    }

    public void testTheConfiguredRateStaysWhenItIsAboveTheFloor() {
        CaptureRate rate = new CaptureRate(0.5, 3600);

        rate.observe(500, 10); // the floor would be 2%
        assertThat(rate.effective(), equalTo(0.5));
    }

    public void testWithoutTrafficWhateverArrivesIsWanted() {
        CaptureRate rate = new CaptureRate(0.01, 100);

        rate.observe(0, 10);
        assertThat(rate.effective(), equalTo(1.0));
    }

    public void testNeverAboveOneAndNoFloorWithoutAMinimum() {
        CaptureRate floorless = new CaptureRate(0.3, 0);
        floorless.observe(1, 10);
        assertThat("no minimum, no floor", floorless.effective(), equalTo(0.3));

        CaptureRate capped = new CaptureRate(0.3, 1_000_000);
        capped.observe(1, 10);
        assertThat(capped.effective(), equalTo(1.0));
    }

    public void testTheTrafficIsSmoothed() {
        CaptureRate rate = new CaptureRate(0.0, 3600);
        rate.observe(500, 10); // 50 a second
        rate.observe(1000, 10); // 100 a second: 0.3 of it and 0.7 of the earlier 50
        double smoothed = 0.3 * 100 + 0.7 * 50;

        assertThat(rate.effective(), closeTo(1.0 / smoothed, 1e-12));
    }

    public void testSettingsChangeTheRateAtOnce() {
        CaptureRate rate = new CaptureRate(0.1, 0);
        rate.observe(500, 10);

        rate.configured(0.4);
        assertThat(rate.effective(), equalTo(0.4));

        rate.minPerHour(3600); // a floor of 2% is below the configured rate
        assertThat(rate.effective(), equalTo(0.4));
        rate.minPerHour(36_000); // a floor of 20% still is
        assertThat(rate.effective(), equalTo(0.4));
        rate.configured(0.01);
        assertThat("now the floor is higher", rate.effective(), closeTo(0.2, 1e-12));
    }

    public void testForgettingTheTrafficRemovesTheFloor() {
        CaptureRate rate = new CaptureRate(0.01, 3600);
        rate.observe(10, 10);
        assertThat(rate.effective(), closeTo(1.0, 1e-12));

        rate.forgetTraffic();
        assertThat(rate.effective(), equalTo(0.01));
    }
}
