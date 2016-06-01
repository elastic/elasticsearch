/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.equalTo;

public class ExpirationCallbackTests extends ESTestCase {

    public void testPostExpiration() throws Exception {
        int postExpirySeconds = randomIntBetween(5, 10);
        TimeValue postExpiryDuration = TimeValue.timeValueSeconds(postExpirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(postExpirySeconds - randomIntBetween(1, 3));
        TimeValue max = TimeValue.timeValueSeconds(postExpirySeconds + randomIntBetween(1, 10));

        final ExpirationCallback.Post post = new ExpirationCallback.Post(min, max, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(post.matches(now - postExpiryDuration.millis(), now), equalTo(true));
        assertThat(post.matches(now + postExpiryDuration.getMillis(), now), equalTo(false));
    }

    public void testPostExpirationWithNullMax() throws Exception {
        int postExpirySeconds = randomIntBetween(5, 10);
        TimeValue postExpiryDuration = TimeValue.timeValueSeconds(postExpirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(postExpirySeconds - randomIntBetween(1, 3));

        final ExpirationCallback.Post post = new ExpirationCallback.Post(min, null, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(post.matches(now - postExpiryDuration.millis(), now), equalTo(true));
    }

    public void testPreExpirationWithNullMin() throws Exception {
        int expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));

        final ExpirationCallback.Pre pre = new ExpirationCallback.Pre(null, max, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(pre.matches(expiryDuration.millis() + now, now), equalTo(true));
    }

    public void testPreExpiration() throws Exception {
        int expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(expirySeconds - randomIntBetween(0, 3));
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));

        final ExpirationCallback.Pre pre = new ExpirationCallback.Pre(min, max, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(pre.matches(expiryDuration.millis() + now, now), equalTo(true));
        assertThat(pre.matches(now - expiryDuration.getMillis(), now), equalTo(false));
    }

    public void testPreExpirationMatchSchedule() throws Exception {
        long expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(expirySeconds - randomIntBetween(0, 3));
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));
        final ExpirationCallback.Pre pre = new ExpirationCallback.Pre(min, max, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long expiryDate = System.currentTimeMillis() + expiryDuration.getMillis();
        final SchedulerEngine.Schedule schedule = pre.schedule(expiryDate);
        final long now = expiryDate - max.millis() + randomIntBetween(1, ((int) min.getMillis()));
        assertThat(schedule.nextScheduledTimeAfter(0, now), equalTo(now + pre.frequency().getMillis()));
        assertThat(schedule.nextScheduledTimeAfter(now, now), equalTo(now));
    }

    public void testPreExpirationNotMatchSchedule() throws Exception {
        long expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(expirySeconds - randomIntBetween(0, 3));
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));
        final ExpirationCallback.Pre pre = new ExpirationCallback.Pre(min, max, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long expiryDate = System.currentTimeMillis() + expiryDuration.getMillis();
        final SchedulerEngine.Schedule schedule = pre.schedule(expiryDate);
        int delta = randomIntBetween(1, 1000);
        final long now = expiryDate - max.millis() - delta;
        assertThat(schedule.nextScheduledTimeAfter(now, now), equalTo(now + delta));
        assertThat(schedule.nextScheduledTimeAfter(1, now), equalTo(-1L));
    }

    public void testPostExpirationMatchSchedule() throws Exception {
        long expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(expirySeconds - randomIntBetween(0, 3));
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));
        final ExpirationCallback.Post post = new ExpirationCallback.Post(min, max, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long expiryDate = System.currentTimeMillis() + expiryDuration.getMillis();
        final SchedulerEngine.Schedule schedule = post.schedule(expiryDate);
        final long now = expiryDate + min.millis() + randomIntBetween(1, ((int) (max.getMillis() - min.getMillis())));
        assertThat(schedule.nextScheduledTimeAfter(0, now), equalTo(now + post.frequency().getMillis()));
        assertThat(schedule.nextScheduledTimeAfter(now, now), equalTo(now));
    }

    public void testPostExpirationNotMatchSchedule() throws Exception {
        long expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(expirySeconds - randomIntBetween(0, 3));
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));
        final ExpirationCallback.Post post = new ExpirationCallback.Post(min, max, timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long expiryDate = System.currentTimeMillis() + expiryDuration.getMillis();
        final SchedulerEngine.Schedule schedule = post.schedule(expiryDate);
        int delta = randomIntBetween(1, 1000);
        final long now = expiryDate - delta;
        assertThat(schedule.nextScheduledTimeAfter(expiryDate, expiryDate), equalTo(expiryDate + min.getMillis()));
        assertThat(schedule.nextScheduledTimeAfter(now, now), equalTo(expiryDate + min.getMillis()));
        assertThat(schedule.nextScheduledTimeAfter(1, now), equalTo(-1L));
    }
}