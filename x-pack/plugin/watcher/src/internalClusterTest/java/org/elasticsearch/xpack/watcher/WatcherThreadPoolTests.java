/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.SystemIndexThreadPoolTestCase;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.transport.actions.QueryWatchesAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatch;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.test.LocalStateWatcher;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.noneInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WatcherThreadPoolTests extends SystemIndexThreadPoolTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Set.of(LocalStateWatcher.class, IndexLifecycle.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // if watcher is running, it may try to use blocked threads
            .put(XPackSettings.WATCHER_ENABLED.getKey(), true)
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(super.getMockPlugins());
        // security has its own transport service
        plugins.remove(MockTransportService.TestPlugin.class);
        // security has its own transport
        // we have to explicitly add it otherwise we will fail to set the check_index_on_close setting
        plugins.add(MockFSIndexStore.TestPlugin.class);
        return plugins;
    }

    /**
     * The main watcher index (.watches) can be tested through the watcher API
     */
    public void testWatcherThreadPools() {
        runWithBlockedThreadPools(() -> {
            {
                // write
                var response = new PutWatchRequestBuilder(client(), "test-watch").setSource(
                    watchBuilder().trigger(schedule(interval("3m"))).input(noneInput()).condition(InternalAlwaysCondition.INSTANCE)
                ).get();
                assertTrue(response.isCreated());
            }

            {
                // get
                var response = new GetWatchRequestBuilder(client()).setId("test-watch").get();
                assertThat(response.getId(), equalTo("test-watch"));
            }

            {
                // search
                var request = new QueryWatchesAction.Request(null, null, new TermQueryBuilder("_id", "test-watch"), null, null);
                var response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
                assertThat(response.getWatchTotalCount(), equalTo(1L));
            }
        });
    }

    /**
     * This test uses an instance of TriggeredWatchStore directly because the public
     * API doesn't seem to return much information that indicates changes in the
     * underlying index.
     */
    public void testTriggeredWatchesIndex() {
        internalCluster().getInstances(TriggerService.class).forEach(TriggerService::pauseExecution);

        TriggeredWatchStore watchStore = internalCluster().getInstance(TriggeredWatchStore.class);

        List<Watch> fakeWatches = getFakeWatches(List.of("fake-patek-phillipe", "fake-tag-heuer"));
        List<Wid> wids = fakeWatches.stream().map(w -> new Wid(w.id(), ZonedDateTime.now())).toList();

        runWithBlockedThreadPools(() -> {
            try {
                BulkResponse bulkResponse = watchStore.putAll(
                    wids.stream()
                        .map(w -> new TriggeredWatch(w, new ScheduleTriggerEvent(ZonedDateTime.now(), ZonedDateTime.now())))
                        .toList()
                );
                assertFalse(bulkResponse.hasFailures());
                assertBusy(() -> {
                    Collection<TriggeredWatch> triggeredWatches1 = watchStore.findTriggeredWatches(fakeWatches, clusterService().state());
                    assertThat(triggeredWatches1, hasSize(2));
                });

                wids.forEach(watchStore::delete);

                assertBusy(() -> {
                    Collection<TriggeredWatch> triggeredWatches2 = watchStore.findTriggeredWatches(fakeWatches, clusterService().state());
                    assertThat(triggeredWatches2, hasSize(0));
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                internalCluster().getInstances(TriggerService.class).forEach(triggerService -> triggerService.start(List.of()));
            }
        });
    }

    private static List<Watch> getFakeWatches(List<String> watchNames) {
        return watchNames.stream().map(n -> {
            Watch fakeWatch = mock(Watch.class);
            when(fakeWatch.id()).thenReturn(n);
            return fakeWatch;
        }).toList();
    }
}
