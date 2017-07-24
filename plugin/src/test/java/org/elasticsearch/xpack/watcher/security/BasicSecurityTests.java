/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.security;

import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.watcher.WatcherState;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import java.util.Collections;

import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken
        .BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken
        .basicAuthHeaderValue;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.joda.time.DateTimeZone.UTC;

public class BasicSecurityTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean enableSecurity() {
        return true;
    }

    public void testNoAuthorization() throws Exception {
        String basicAuth = basicAuthHeaderValue("transport_client", SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING);
        WatcherClient watcherClient = watcherClient().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuth));
        Exception e = expectThrows(Exception.class, () -> watcherClient.prepareWatcherStats().get());
        assertThat(e.getMessage(),
                   equalTo("action [cluster:monitor/xpack/watcher/stats/dist] is unauthorized for user [transport_client]"));
    }

    public void testWatcherMonitorRole() throws Exception {
        // stats and get watch apis require at least monitor role:
        String token = basicAuthHeaderValue("test", SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING);
        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareWatcherStats()
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:monitor/xpack/watcher/stats/dist] is unauthorized for user [test]"));
        }

        try {
            watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareGetWatch("_id")
                    .get();
            fail("authentication failure should have occurred");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("action [cluster:monitor/xpack/watcher/watch/get] is unauthorized for user [test]"));
        }

        // stats and get watch are allowed by role monitor:
        token = basicAuthHeaderValue("monitor", SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING);
        WatcherStatsResponse statsResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareWatcherStats().get();
        boolean watcherStarted = statsResponse.getNodes().stream().anyMatch(node -> node.getWatcherState() == WatcherState.STARTED);
        assertThat(watcherStarted, is(true));
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
        String token = basicAuthHeaderValue("test", SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING);
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
        token = basicAuthHeaderValue("admin", SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING);
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
        token = basicAuthHeaderValue("admin",SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING);
        WatcherStatsResponse statsResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareWatcherStats()
                .get();
        boolean watcherStarted = statsResponse.getNodes().stream().anyMatch(node -> node.getWatcherState() == WatcherState.STARTED);
        assertThat(watcherStarted, is(true));
        GetWatchResponse getWatchResponse = watcherClient().filterWithHeader(Collections.singletonMap("Authorization", token))
                .prepareGetWatch("_id")
                .get();
        assertThat(getWatchResponse.isFound(), is(false));
    }
}
