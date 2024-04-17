/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexThreadPoolTestCase;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.LocalStateWatcher;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.noneInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;

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

    public void testWatcherThreadPools() {
        runWithBlockedThreadPools(() -> {
            var response = new PutWatchRequestBuilder(client(), "test-watch").setSource(
                watchBuilder().trigger(schedule(interval("3s")))
                    .input(noneInput())
                    .condition(InternalAlwaysCondition.INSTANCE)
                    .addAction("indexer", indexAction("test-index"))
            ).get();
            assertTrue(response.isCreated());
        });
    }
}
