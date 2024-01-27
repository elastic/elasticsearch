/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.ingest;

import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class BulkProcessorFactoryTests extends ESTestCase {
    private static TestThreadPool testThreadPool;

    @BeforeClass
    public static void beforeCLass() {
        testThreadPool = new TestThreadPool("BulkProcessorRunnerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
    }

    public void testDefaultConstructor() throws Exception {
        BulkProcessorFactory factory = new BulkProcessorFactory(mock(Client.class), mock(AnalyticsEventIngestConfig.class));
        assertThat(factory.create(), instanceOf(BulkProcessor2.class));
    }

    public void testConfigValueAreUsed() throws Exception {
        TimeValue flushDelay = TimeValue.parseTimeValue(randomTimeValue(), "random time value");
        int maxBulkActions = randomIntBetween(1, 10);
        int numberOfRetries = between(0, 5);
        ByteSizeValue maxBytesInFlight = randomByteSizeValue();

        AnalyticsEventIngestConfig config = mock(AnalyticsEventIngestConfig.class);
        doReturn(flushDelay).when(config).flushDelay();
        doReturn(maxBulkActions).when(config).maxNumberOfEventsPerBulk();
        doReturn(maxBytesInFlight).when(config).maxBytesInFlight();
        doReturn(numberOfRetries).when(config).maxNumberOfRetries();

        BulkProcessor2.Builder baseBuilder = spy(BulkProcessor2.builder(mock(), new BulkProcessorFactory.BulkProcessorListener(), mock()));
        BulkProcessorFactory factory = new BulkProcessorFactory(config, () -> baseBuilder);

        assertThat(factory.create(), instanceOf(BulkProcessor2.class));

        verify(baseBuilder).setFlushInterval(eq(flushDelay));
        verify(baseBuilder).setBulkActions(eq(maxBulkActions));
        verify(baseBuilder).setMaxNumberOfRetries(numberOfRetries);
        verify(baseBuilder).setMaxBytesInFlight(maxBytesInFlight);
    }
}
