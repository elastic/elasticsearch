/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.AbstractOldXPackIndicesBackwardsCompatibilityTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;

import java.util.Map;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Tests for watcher indexes created before 5.0.
 */
public class OldWatcherIndicesBackwardsCompatibilityTests extends AbstractOldXPackIndicesBackwardsCompatibilityTestCase {
    @Override
    public Settings nodeSettings(int ord) {
        return Settings.builder()
                .put(super.nodeSettings(ord))
                .put(XPackSettings.WATCHER_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected void checkVersion(Version version) throws Exception {
        // Wait for watcher to actually start....
        assertBusy(() -> {
            assertEquals(WatcherState.STARTED, internalCluster().getInstance(WatcherService.class).state());
        });
        try {
            assertWatchIndexContentsWork(version);
            assertBasicWatchInteractions();
        } finally {
            /* Shut down watcher after every test because watcher can be a bit finicky about shutting down when the node shuts down. This
             * makes super sure it shuts down *and* causes the test to fail in a sensible spot if it doesn't shut down. */
            internalCluster().getInstance(WatcherLifeCycleService.class).stop();
            assertBusy(() -> {
                assertEquals(WatcherState.STOPPED, internalCluster().getInstance(WatcherService.class).state());
            });
        }
    }

    void assertWatchIndexContentsWork(Version version) throws Exception {
        WatcherClient watcherClient = new WatcherClient(client());

        // Fetch a basic watch
        GetWatchResponse bwcWatch = watcherClient.prepareGetWatch("bwc_watch").get();
        assertTrue(bwcWatch.isFound());
        assertNotNull(bwcWatch.getSource());
        Map<String, Object> source = bwcWatch.getSource().getAsMap();
        assertEquals(1000, source.get("throttle_period_in_millis"));
        Map<?, ?> input = (Map<?, ?>) source.get("input");
        Map<?, ?> search = (Map<?, ?>) input.get("search");
        // We asked for 100s but 2.x converted that to 1.6m which is actually 96s...
        int timeout = (int) (version.onOrAfter(Version.V_5_0_0_alpha1) ? timeValueSeconds(100).millis() : timeValueSeconds(96).millis());
        assertEquals(timeout, search.get("timeout_in_millis"));
        Map<?, ?> actions = (Map<?, ?>) source.get("actions");
        Map<?, ?> indexPayload = (Map<?, ?>) actions.get("index_payload");
        Map<?, ?> transform = (Map<?, ?>) indexPayload.get("transform");
        search = (Map<?, ?>) transform.get("search");
        assertEquals(timeout, search.get("timeout_in_millis"));
        Map<?, ?> index = (Map<?, ?>) indexPayload.get("index");
        assertEquals("bwc_watch_index", index.get("index"));
        assertEquals("bwc_watch_type", index.get("doc_type"));
        assertEquals(timeout, index.get("timeout_in_millis"));

        // Fetch a watch with "fun" throttle periods
        bwcWatch = watcherClient.prepareGetWatch("bwc_throttle_period").get();
        assertTrue(bwcWatch.isFound());
        assertNotNull(bwcWatch.getSource());
        source = bwcWatch.getSource().getAsMap();
        assertEquals(timeout, source.get("throttle_period_in_millis"));
        actions = (Map<?, ?>) source.get("actions");
        indexPayload = (Map<?, ?>) actions.get("index_payload");
        assertEquals(timeout, indexPayload.get("throttle_period_in_millis"));

        if (version.onOrAfter(Version.V_2_3_0)) {
            /* Fetch a watch with a funny timeout to verify loading fractional time values. This watch is only built in >= 2.3 because
             * email attachments aren't supported before that. */
            bwcWatch = watcherClient.prepareGetWatch("bwc_funny_timeout").get();
            assertTrue(bwcWatch.isFound());
            assertNotNull(bwcWatch.getSource());
            source = bwcWatch.getSource().getAsMap();
            actions = (Map<?, ?>) source.get("actions");
            Map<?, ?> work = (Map<?, ?>) actions.get("work");
            Map<?, ?> email = (Map<?, ?>) work.get("email");
            Map<?, ?> attachments = (Map<?, ?>) email.get("attachments");
            Map<?, ?> attachment = (Map<?, ?>) attachments.get("test_report.pdf");
            Map<?, ?> http = (Map<?, ?>) attachment.get("http");
            Map<?, ?> request = (Map<?, ?>) http.get("request");
            assertEquals(timeout, request.get("read_timeout_millis"));
            assertEquals("https", request.get("scheme"));
            assertEquals("example.com", request.get("host"));
            assertEquals("{{ctx.metadata.report_url}}", request.get("path"));
            assertEquals(8443, request.get("port"));
            Map<?, ?> auth = (Map<?, ?>) request.get("auth");
            Map<?, ?> basic = (Map<?, ?>) auth.get("basic");
            assertThat(basic, hasEntry("username", "Aladdin"));
            // password doesn't come back because it is hidden
            assertThat(basic, not(hasKey("password")));
        }

        String watchHistoryPattern = version.onOrAfter(Version.V_5_0_0_alpha1) ? ".watcher-history*" : ".watch_history*";
        SearchResponse history = client().prepareSearch(watchHistoryPattern).get();
        assertThat(history.getHits().getTotalHits(), greaterThanOrEqualTo(10L));
    }

    void assertBasicWatchInteractions() throws Exception {
        WatcherClient watcherClient = new WatcherClient(client());

        PutWatchResponse put = watcherClient.preparePutWatch("new_watch").setSource(new WatchSourceBuilder()
                .condition(AlwaysCondition.INSTANCE)
                .trigger(ScheduleTrigger.builder(new IntervalSchedule(Interval.seconds(1))))
                .addAction("awesome", LoggingAction.builder(new TextTemplate("test")))).get();
        assertTrue(put.isCreated());
        assertEquals(1, put.getVersion());

        put = watcherClient.preparePutWatch("new_watch").setSource(new WatchSourceBuilder()
                .condition(AlwaysCondition.INSTANCE)
                .trigger(ScheduleTrigger.builder(new IntervalSchedule(Interval.seconds(1))))
                .addAction("awesome", LoggingAction.builder(new TextTemplate("test")))).get();
        assertFalse(put.isCreated());
        assertEquals(2, put.getVersion());

        GetWatchResponse get = watcherClient.prepareGetWatch(put.getId()).get();
        assertTrue(get.isFound());
        {
            Map<?, ?> source = get.getSource().getAsMap();
            Map<?, ?> actions = (Map<?, ?>) source.get("actions");
            Map<?, ?> awesome = (Map<?, ?>) actions.get("awesome");
            Map<?, ?> logging = (Map<?, ?>) awesome.get("logging");
            assertEquals("info", logging.get("level"));
            assertEquals("test", logging.get("text"));
        }
    }

}
