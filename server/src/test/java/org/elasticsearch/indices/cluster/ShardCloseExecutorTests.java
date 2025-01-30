/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardCloseExecutorTests extends ESTestCase {

    public void testThrottling() {
        final var defaultProcessors = EsExecutors.NODE_PROCESSORS_SETTING.get(Settings.EMPTY).roundUp();
        ensureThrottling(Math.min(10, defaultProcessors), Settings.EMPTY);

        if (10 < defaultProcessors) {
            ensureThrottling(
                10,
                Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), between(10, defaultProcessors - 1)).build()
            );
        }

        if (1 < defaultProcessors) {
            final var fewProcessors = between(1, Math.min(10, defaultProcessors - 1));
            ensureThrottling(fewProcessors, Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), fewProcessors).build());
        }

        final var override = between(1, defaultProcessors * 2);
        ensureThrottling(
            override,
            Settings.builder().put(IndicesClusterStateService.CONCURRENT_SHARD_CLOSE_LIMIT.getKey(), override).build()
        );
    }

    private static void ensureThrottling(int expectedLimit, Settings settings) {
        final var tasksToRun = new ArrayList<Runnable>(expectedLimit + 1);
        final var executor = new IndicesClusterStateService.ShardCloseExecutor(settings, tasksToRun::add);
        final var runCount = new AtomicInteger();

        for (int i = 0; i < expectedLimit + 1; i++) {
            executor.execute(runCount::incrementAndGet);
        }

        assertEquals(expectedLimit, tasksToRun.size()); // didn't enqueue the final task yet

        for (int i = 0; i < tasksToRun.size(); i++) {
            assertEquals(i, runCount.get());
            tasksToRun.get(i).run();
            assertEquals(i + 1, runCount.get());
            assertEquals(expectedLimit + 1, tasksToRun.size());
        }
    }
}
