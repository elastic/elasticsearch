/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.OptionalLong;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ScaleTimerTests extends ESTestCase {

    public void testLastScaleToScaleIntervalMillis_GivenNoScaleEver() {
        ScaleTimer scaleTimer = new ScaleTimer(() -> System.currentTimeMillis());

        assertThat(scaleTimer.lastScaleToScaleIntervalMillis().isEmpty(), is(true));
    }

    public void testLastScaleToScaleIntervalMillis_GivenSingleScaleEvent() {
        ScaleTimer scaleTimer = new ScaleTimer(() -> System.currentTimeMillis());

        scaleTimer.markScale();

        assertThat(scaleTimer.lastScaleToScaleIntervalMillis().isEmpty(), is(true));
    }

    public void testLastScaleToScaleIntervalMillis_GivenMultipleScaleEvents() {
        ScaleTimer scaleTimer = new ScaleTimer(new MockNowSupplier(100L, 250L, 500L));

        scaleTimer.markScale();
        scaleTimer.markScale();

        OptionalLong scaleInterval = scaleTimer.lastScaleToScaleIntervalMillis();
        assertThat(scaleInterval.isPresent(), is(true));
        assertThat(scaleInterval.getAsLong(), equalTo(150L));

        scaleTimer.markScale();

        scaleInterval = scaleTimer.lastScaleToScaleIntervalMillis();
        assertThat(scaleInterval.isPresent(), is(true));
        assertThat(scaleInterval.getAsLong(), equalTo(250L));
    }

    public void testMarkDownScaleAndGetMillisLeftFromDelay() {
        ScaleTimer scaleTimer = new ScaleTimer(new MockNowSupplier(100L, 100L, 300L, 1300L, 1500L));
        scaleTimer.markScale();

        long millisLeft = scaleTimer.markDownScaleAndGetMillisLeftFromDelay(Settings.builder().put("down_scale_delay", "1s").build());
        assertThat(scaleTimer.downScaleDetectedMillis(), equalTo(100L));
        assertThat(millisLeft, equalTo(1000L));

        millisLeft = scaleTimer.markDownScaleAndGetMillisLeftFromDelay(Settings.builder().put("down_scale_delay", "1s").build());
        assertThat(scaleTimer.downScaleDetectedMillis(), equalTo(100L));
        assertThat(millisLeft, equalTo(800L));

        millisLeft = scaleTimer.markDownScaleAndGetMillisLeftFromDelay(Settings.builder().put("down_scale_delay", "1s").build());
        assertThat(scaleTimer.downScaleDetectedMillis(), equalTo(100L));
        assertThat(millisLeft, equalTo(-200L));

        scaleTimer.resetScaleDownCoolDown();

        millisLeft = scaleTimer.markDownScaleAndGetMillisLeftFromDelay(Settings.builder().put("down_scale_delay", "1s").build());
        assertThat(scaleTimer.downScaleDetectedMillis(), equalTo(1500L));
        assertThat(millisLeft, equalTo(1000L));
    }

    private class MockNowSupplier implements LongSupplier {

        private final long[] nows;
        private int count = 0;

        private MockNowSupplier(long... nows) {
            this.nows = nows;
        }

        @Override
        public long getAsLong() {
            return nows[count >= nows.length ? nows.length - 1 : count++];
        }
    }
}
