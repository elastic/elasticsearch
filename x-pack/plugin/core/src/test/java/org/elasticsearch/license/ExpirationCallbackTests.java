/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ExpirationCallbackTests extends ESTestCase {

    public void testPostExpirationDelay() throws Exception {
        TimeValue expiryDuration = TimeValue.timeValueSeconds(randomIntBetween(5, 10));
        TimeValue min = TimeValue.timeValueSeconds(1);
        TimeValue max = TimeValue.timeValueSeconds(4);
        TimeValue frequency = TimeValue.timeValueSeconds(1);
        NoopPostExpirationCallback post = new NoopPostExpirationCallback(min, max, frequency);
        long now = System.currentTimeMillis();
        long expiryDate = now + expiryDuration.getMillis();
        assertThat(post.delay(expiryDate, now),
            // before license expiry
            equalTo(TimeValue.timeValueMillis(expiryDuration.getMillis() + min.getMillis())));
        assertThat(post.delay(expiryDate, expiryDate), equalTo(min)); // on license expiry
        int latestValidTriggerDelay = (int) (expiryDuration.getMillis() + max.getMillis());
        int earliestValidTriggerDelay = (int) (expiryDuration.getMillis() + min.getMillis());
        assertExpirationCallbackDelay(post, expiryDuration.millis(), latestValidTriggerDelay, earliestValidTriggerDelay);
    }

    public void testPreExpirationDelay() throws Exception {
        TimeValue expiryDuration = TimeValue.timeValueSeconds(randomIntBetween(5, 10));
        TimeValue min = TimeValue.timeValueSeconds(1);
        TimeValue max = TimeValue.timeValueSeconds(4);
        TimeValue frequency = TimeValue.timeValueSeconds(1);
        NoopPreExpirationCallback pre = new NoopPreExpirationCallback(min, max, frequency);
        long now = System.currentTimeMillis();
        long expiryDate = now + expiryDuration.getMillis();
        assertThat(pre.delay(expiryDate, expiryDate), nullValue()); // on license expiry
        int latestValidTriggerDelay = (int) (expiryDuration.getMillis() - min.getMillis());
        int earliestValidTriggerDelay = (int) (expiryDuration.getMillis() - max.getMillis());
        assertExpirationCallbackDelay(pre, expiryDuration.millis(), latestValidTriggerDelay, earliestValidTriggerDelay);
    }

    public void testPostExpirationWithNullMax() throws Exception {
        int postExpirySeconds = randomIntBetween(5, 10);
        TimeValue postExpiryDuration = TimeValue.timeValueSeconds(postExpirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(postExpirySeconds - randomIntBetween(1, 3));

        final ExpirationCallback.Post post = new NoopPostExpirationCallback(min, null, timeValueMillis(10));
        long now = System.currentTimeMillis();
        assertThat(post.delay(now - postExpiryDuration.millis(), now), equalTo(TimeValue.timeValueMillis(0)));
    }

    public void testPreExpirationWithNullMin() throws Exception {
        int expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));

        final ExpirationCallback.Pre pre = new NoopPreExpirationCallback(null, max, timeValueMillis(10));
        long now = System.currentTimeMillis();
        assertThat(pre.delay(expiryDuration.millis() + now, now), equalTo(TimeValue.timeValueMillis(0)));
    }

    public void testPreExpirationScheduleTime() throws Exception {
        TimeValue expiryDuration = TimeValue.timeValueSeconds(randomIntBetween(5, 10));
        TimeValue min = TimeValue.timeValueSeconds(1);
        TimeValue max = TimeValue.timeValueSeconds(4);
        TimeValue frequency = TimeValue.timeValueSeconds(1);
        NoopPreExpirationCallback pre = new NoopPreExpirationCallback(min, max, frequency);
        int latestValidTriggerDelay = (int) (expiryDuration.getMillis() - min.getMillis());
        int earliestValidTriggerDelay = (int) (expiryDuration.getMillis() - max.getMillis());
        assertExpirationCallbackScheduleTime(pre, expiryDuration.millis(), latestValidTriggerDelay, earliestValidTriggerDelay);
    }

    public void testPostExpirationScheduleTime() throws Exception {
        TimeValue expiryDuration = TimeValue.timeValueSeconds(randomIntBetween(5, 10));
        TimeValue min = TimeValue.timeValueSeconds(1);
        TimeValue max = TimeValue.timeValueSeconds(4);
        TimeValue frequency = TimeValue.timeValueSeconds(1);
        NoopPostExpirationCallback pre = new NoopPostExpirationCallback(min, max, frequency);
        int latestValidTriggerDelay = (int) (expiryDuration.getMillis() + max.getMillis());
        int earliestValidTriggerDelay = (int) (expiryDuration.getMillis() + min.getMillis());
        assertExpirationCallbackScheduleTime(pre, expiryDuration.millis(), latestValidTriggerDelay, earliestValidTriggerDelay);
    }

    private void assertExpirationCallbackDelay(ExpirationCallback expirationCallback, long expiryDuration,
                                               int latestValidTriggerDelay, int earliestValidTriggerDelay) {
        long now = System.currentTimeMillis();
        long expiryDate = now + expiryDuration;
        // bounds
        assertThat(expirationCallback.delay(expiryDate, now + earliestValidTriggerDelay), equalTo(TimeValue.timeValueMillis(0)));
        assertThat(expirationCallback.delay(expiryDate, now + latestValidTriggerDelay), equalTo(TimeValue.timeValueMillis(0)));
        // in match
        assertThat(expirationCallback.delay(expiryDate,
                now + randomIntBetween(earliestValidTriggerDelay, latestValidTriggerDelay)),
                equalTo(TimeValue.timeValueMillis(0)));
        // out of bounds
        int deltaBeforeEarliestMatch = between(1, earliestValidTriggerDelay);
        assertThat(expirationCallback.delay(expiryDate, now + deltaBeforeEarliestMatch),
                equalTo(TimeValue.timeValueMillis(earliestValidTriggerDelay - deltaBeforeEarliestMatch)));
        int deltaAfterLatestMatch = between(latestValidTriggerDelay + 1, Integer.MAX_VALUE); // after expiry and after max
        assertThat(expirationCallback.delay(expiryDate, expiryDate + deltaAfterLatestMatch), nullValue());
    }

    public void assertExpirationCallbackScheduleTime(ExpirationCallback expirationCallback, long expiryDuration,
                                                      int latestValidTriggerDelay, int earliestValidTriggerDelay) {
        long now = System.currentTimeMillis();
        long expiryDate = now + expiryDuration;
        int validTriggerInterval = between(earliestValidTriggerDelay, latestValidTriggerDelay);
        assertThat(expirationCallback.nextScheduledTimeForExpiry(expiryDate,
                now + validTriggerInterval, now + validTriggerInterval),
                equalTo(now + validTriggerInterval));
        assertThat(expirationCallback.nextScheduledTimeForExpiry(expiryDate, now, now + validTriggerInterval),
                equalTo(now + validTriggerInterval + expirationCallback.getFrequency()));

        int deltaBeforeEarliestMatch = between(1, earliestValidTriggerDelay - 1);
        assertThat(expirationCallback.nextScheduledTimeForExpiry(expiryDate, now, now + deltaBeforeEarliestMatch),
                equalTo(now + deltaBeforeEarliestMatch +
                        expirationCallback.delay(expiryDate, now + deltaBeforeEarliestMatch).getMillis()));
        assertThat(expirationCallback.nextScheduledTimeForExpiry(expiryDate,
                now + deltaBeforeEarliestMatch, now + deltaBeforeEarliestMatch),
                equalTo(now + deltaBeforeEarliestMatch +
                        expirationCallback.delay(expiryDate, now + deltaBeforeEarliestMatch).getMillis()));

        int deltaAfterLatestMatch = between(latestValidTriggerDelay + 1, Integer.MAX_VALUE); // after expiry and after max
        assertThat(expirationCallback.nextScheduledTimeForExpiry(expiryDate, now, now + deltaAfterLatestMatch), equalTo(-1L));
        assertThat(expirationCallback.nextScheduledTimeForExpiry(expiryDate,
                now + deltaAfterLatestMatch, now + deltaAfterLatestMatch),
                equalTo(-1L));
    }

    private static class NoopPostExpirationCallback extends ExpirationCallback.Post {

        NoopPostExpirationCallback(TimeValue min, TimeValue max, TimeValue frequency) {
            super(min, max, frequency);
        }

        @Override
        public void on(License license) {}
    }

    private static class NoopPreExpirationCallback extends ExpirationCallback.Pre {

        NoopPreExpirationCallback(TimeValue min, TimeValue max, TimeValue frequency) {
            super(min, max, frequency);
        }

        @Override
        public void on(License license) {}
    }
}
