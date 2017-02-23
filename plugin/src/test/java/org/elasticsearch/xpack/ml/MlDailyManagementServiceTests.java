/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.mockito.Mockito.mock;

public class MlDailyManagementServiceTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpTests() {
        threadPool = new TestThreadPool("MlDailyManagementServiceTests");
    }

    @After
    public void stop() throws InterruptedException {
        terminate(threadPool);
    }

    public void testScheduledTriggering() throws InterruptedException {
        MlDailyManagementService.Listener listener1 = mock(MlDailyManagementService.Listener.class);
        MlDailyManagementService.Listener listener2 = mock(MlDailyManagementService.Listener.class);
        int triggerCount = randomIntBetween(2, 4);
        CountDownLatch latch = new CountDownLatch(triggerCount);
        try (MlDailyManagementService service = createService(latch, Arrays.asList(listener1, listener2))) {
            service.start();
            latch.await(1, TimeUnit.SECONDS);
        }

        verify(listener1, org.mockito.Mockito.atLeast(triggerCount - 1)).onTrigger();
        verify(listener2, org.mockito.Mockito.atLeast(triggerCount - 1)).onTrigger();
    }

    private MlDailyManagementService createService(CountDownLatch latch, List<MlDailyManagementService.Listener> listeners) {
        return new MlDailyManagementService(threadPool, listeners, () -> {
                latch.countDown();
                return TimeValue.timeValueMillis(100);
            });
    }
}