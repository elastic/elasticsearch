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
        // This defaults to the number of CPUs of the machine running the tests which could be either side of 10.
        final var defaultProcessors = EsExecutors.NODE_PROCESSORS_SETTING.get(Settings.EMPTY).roundUp();
        ensureThrottling(Math.min(10, defaultProcessors), Settings.EMPTY);

        if (10 < defaultProcessors) {
            ensureThrottling(
                10,
                Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), between(10, defaultProcessors - 1)).build()
            );
        } // else we cannot run this check, the machine running the tests doesn't have enough CPUs

        if (1 < defaultProcessors) {
            final var fewProcessors = between(1, Math.min(10, defaultProcessors - 1));
            ensureThrottling(fewProcessors, Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), fewProcessors).build());
        } // else we cannot run this check, the machine running the tests has less than 2 whole CPUs (and we already tested the 1 case)

        // but in any case we can override the throttle regardless of its default value
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

        // enqueue one more task than the throttling limit
        for (int i = 0; i < expectedLimit + 1; i++) {
            executor.execute(runCount::incrementAndGet);
        }

        // check that we submitted tasks up to the expected limit, holding back the final task behind the throttle for now
        assertEquals(expectedLimit, tasksToRun.size());

        // now execute all the tasks one by one
        for (int i = 0; i < expectedLimit + 1; i++) {
            assertEquals(i, runCount.get());
            tasksToRun.get(i).run();
            assertEquals(i + 1, runCount.get());

            // executing the first task enqueues the final task
            assertEquals(expectedLimit + 1, tasksToRun.size());
        }
    }
}
