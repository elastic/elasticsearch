/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.shield;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.watcher.WatcherState;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import java.util.Collections;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.joda.time.DateTimeZone.UTC;

public class BasicShieldTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean enableShield() {
        return true;
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put("client.transport.sniff", false)
                // Use just the transport user here, so we can test Watcher roles specifically
                .put(Security.USER_SETTING.getKey(), "transport_client:changeme")
                .build();
    }

    public void testNoAuthorization() throws Exception {
        try {
            watcherClient().prepareWatcherStats().get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            // transport_client is the default user
            assertThat(e.getMessage(), equalTo("action [cluster:monitor/xpack/watcher/stats] is unauthorized for user [transport_client]"));
        }
    }

    public void testWatcherMonitorRole() throws Exception {
        // stats and get watch apis require at least monitor role:
        String token = basicAuthHeaderValue("test", new SecuredString("changeme".toCharArray()));
        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareWatcherStats()
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:monitor/xpack/watcher/stats] is unauthorized for user [test]"));
        }

        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareGetWatch("_id")
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:monitor/xpack/watcher/watch/get] is unauthorized for user [test]"));
        }

        // stats and get watch are allowed by role monitor:
        token = basicAuthHeaderValue("monitor", new SecuredString("changeme".toCharArray()));
        WatcherStatsResponse statsResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareWatcherStats().get();
        assertThat(statsResponse.getWatcherState(), equalTo(WatcherState.STARTED));
        GetWatchResponse getWatchResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareGetWatch("_id").get();
        assertThat(getWatchResponse.isFound(), is(false));

        // but put watch isn't allowed by monitor:
        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).preparePutWatch("_id")
                    .setSource(watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))))
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:admin/xpack/watcher/watch/put] is unauthorized for user [monitor]"));
        }
    }

    public void testWatcherAdminRole() throws Exception {
        // put, execute and delete watch apis requires watcher admin role:
        String token = basicAuthHeaderValue("test", new SecuredString("changeme".toCharArray()));
        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).preparePutWatch("_id")
                    .setSource(watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))))
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:admin/xpack/watcher/watch/put] is unauthorized for user [test]"));
        }

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC));
        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareExecuteWatch("_id")
                    .setTriggerEvent(triggerEvent)
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:admin/xpack/watcher/watch/execute] is unauthorized for user [test]"));
        }

        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareDeleteWatch("_id")
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:admin/xpack/watcher/watch/delete] is unauthorized for user [test]"));
        }

        // put, execute and delete watch apis are allowed by role admin:
        token = basicAuthHeaderValue("admin", new SecuredString("changeme".toCharArray()));
        PutWatchResponse putWatchResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .preparePutWatch("_id")
                .setSource(watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))))
                .get();
        assertThat(putWatchResponse.getVersion(), equalTo(1L));
        ExecuteWatchResponse executeWatchResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareExecuteWatch("_id")
                .setTriggerEvent(triggerEvent)
                .get();
        DeleteWatchResponse deleteWatchResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareDeleteWatch("_id")
                .get();
        assertThat(deleteWatchResponse.getVersion(), equalTo(2L));
        assertThat(deleteWatchResponse.isFound(), is(true));

        // stats and get watch are also allowed by role monitor:
        token = basicAuthHeaderValue("admin", new SecuredString("changeme".toCharArray()));
        WatcherStatsResponse statsResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareWatcherStats()
                .get();
        assertThat(statsResponse.getWatcherState(), equalTo(WatcherState.STARTED));
        GetWatchResponse getWatchResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareGetWatch("_id")
                .get();
        assertThat(getWatchResponse.isFound(), is(false));
    }
}
