/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.AbstractOldXPackIndicesBackwardsCompatibilityTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Tests for watcher indexes created before 5.0.
 */
@AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/pull/3342")
public class OldWatcherIndicesBackwardsCompatibilityIT extends AbstractOldXPackIndicesBackwardsCompatibilityTestCase {
    @Override
    public Settings nodeSettings(int ord) {
        return Settings.builder()
                .put(super.nodeSettings(ord))
                .put(XPackSettings.WATCHER_ENABLED.getKey(), true)
                .build();
    }

    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (Version v : VersionUtils.allVersions()) {
            if (v.before(Version.V_2_0_0)) continue; // unsupported indexes
            if (v.equals(Version.CURRENT)) continue; // the current version is always compatible with itself
            if (v.isBeta() == true || v.isAlpha() == true || v.isRC() == true) continue; // don't check alphas etc
            expectedVersions.add("x-pack-" + v.toString() + ".zip");
        }
        for (String index : dataFiles) {
            if (expectedVersions.remove(index) == false) {
                logger.warn("Old indexes tests contain extra index: {}", index);
            }
        }
        if (expectedVersions.isEmpty() == false) {
            StringBuilder msg = new StringBuilder("Old index tests are missing indexes:");
            for (String expected : expectedVersions) {
                msg.append("\n" + expected);
            }
            fail(msg.toString());
        }
    }

    @Override
    public void testOldIndexes() throws Exception {
        super.testOldIndexes();
        // Wait for watcher to fully start before shutting down
        assertBusy(() -> {
            assertEquals(WatcherState.STARTED, internalCluster().getInstance(WatcherService.class).state());
        });
        // Shutdown watcher on the last node so that the test can shutdown cleanly
        internalCluster().getInstance(WatcherLifeCycleService.class).stop();
    }

    @Override
    protected void checkVersion(Version version) throws Exception {
        // Wait for watcher to actually start....
        assertBusy(() -> {
            assertEquals(WatcherState.STARTED, internalCluster().getInstance(WatcherService.class).state());
        });
        assertWatchIndexContentsWork(version);
        assertBasicWatchInteractions();
    }

    void assertWatchIndexContentsWork(Version version) throws Exception {
        WatcherClient watcherClient = new WatcherClient(client());

        // Fetch a basic watch
        GetWatchResponse bwcWatch = watcherClient.prepareGetWatch("bwc_watch").get();
        assertTrue(bwcWatch.isFound());
        assertNotNull(bwcWatch.getSource());
        Map<String, Object> source = bwcWatch.getSource().getAsMap();
        Map<?, ?> actions = (Map<?, ?>) source.get("actions");
        Map<?, ?> indexPayload = (Map<?, ?>) actions.get("index_payload");
        Map<?, ?> index = (Map<?, ?>) indexPayload.get("index");
        assertEquals("bwc_watch_index", index.get("index"));
        assertEquals("bwc_watch_type", index.get("doc_type"));

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
            assertEquals(96000, request.get("read_timeout_millis"));
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

        SearchResponse history = client().prepareSearch(".watch_history*").get();
        assertThat(history.getHits().totalHits(), greaterThanOrEqualTo(10L));
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
