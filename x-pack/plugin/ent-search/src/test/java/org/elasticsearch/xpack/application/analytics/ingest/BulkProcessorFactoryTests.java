/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.ingest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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

    public void testFlushDelay() throws Exception {
        AnalyticsEventIngestConfig config = mock(AnalyticsEventIngestConfig.class);
        doReturn(ByteSizeValue.ofMb(10)).when(config).maxBytesInFlight();
        doReturn(TimeValue.timeValueSeconds(1)).when(config).flushDelay();
        doReturn(10).when(config).maxNumberOfEventsPerBulk();

        Client client = mock(Client.class);

        doReturn(testThreadPool).when(client).threadPool();
        BulkProcessor2 bulkProcessor = new BulkProcessorFactory(client, config).create();
        IndexRequest indexRequest = mock(IndexRequest.class);
        bulkProcessor.add(indexRequest);

        assertBusy(() -> verify(client).execute(any(BulkAction.class), argThat((BulkRequest bulkRequest) -> {
            assertThat(bulkRequest.numberOfActions(), equalTo(1));
            assertThat(bulkRequest.requests().stream().findFirst().get(), equalTo(indexRequest));
            return true;
        }), any()), 1, TimeUnit.SECONDS);
    }

    public void testMaxBulkActions() throws InterruptedException {
        int maxBulkActions = randomIntBetween(1, 10);
        int totalEvents = randomIntBetween(1, 5) * maxBulkActions + randomIntBetween(1, maxBulkActions);

        AnalyticsEventIngestConfig config = mock(AnalyticsEventIngestConfig.class);
        doReturn(maxBulkActions).when(config).maxNumberOfEventsPerBulk();
        doReturn(ByteSizeValue.ofMb(10)).when(config).maxBytesInFlight();

        Client client = mock(Client.class);
        InOrder inOrder = Mockito.inOrder(client);

        doReturn(testThreadPool).when(client).threadPool();
        BulkProcessor2 bulkProcessor = new BulkProcessorFactory(client, config).create();

        for (int i = 0; i < totalEvents; i++) {
            bulkProcessor.add(mock(IndexRequest.class));
        }

        inOrder.verify(client, times(totalEvents / maxBulkActions)).execute(any(BulkAction.class), argThat((BulkRequest bulkRequest) -> {
            // Verify a bulk is executed immediately with maxNumberOfEventsPerBulk is reached.
            assertThat(bulkRequest.numberOfActions(), equalTo(maxBulkActions));
            return true;
        }), any());

        bulkProcessor.awaitClose(1, TimeUnit.SECONDS);

        if (totalEvents % maxBulkActions > 0) {
            inOrder.verify(client).execute(any(BulkAction.class), argThat((BulkRequest bulkRequest) -> {
                // Verify another bulk with only 1 event (the remaining) is executed when closing the processor.
                assertThat(bulkRequest.numberOfActions(), equalTo(totalEvents % maxBulkActions));
                return true;
            }), any());
        }
    }

    public void testMaxRetries() {
        int numberOfRetries = between(0, 5);
        AnalyticsEventIngestConfig config = mock(AnalyticsEventIngestConfig.class);
        doReturn(1).when(config).maxNumberOfEventsPerBulk();
        doReturn(numberOfRetries).when(config).maxNumberOfRetries();
        doReturn(ByteSizeValue.ofMb(10)).when(config).maxBytesInFlight();

        Client client = mock(Client.class);
        doAnswer(i -> {
            i.getArgument(2, ActionListener.class).onFailure(new ElasticsearchStatusException("", RestStatus.TOO_MANY_REQUESTS));
            return null;
        }).when(client).execute(any(), any(), any());
        doReturn(testThreadPool).when(client).threadPool();
        BulkProcessor2 bulkProcessor = new BulkProcessorFactory(client, config).create();

        IndexRequest indexRequest = mock(IndexRequest.class);
        bulkProcessor.add(indexRequest);

        verify(client, times(numberOfRetries + 1)).execute(any(BulkAction.class), argThat((BulkRequest bulkRequest) -> {
            assertThat(bulkRequest.numberOfActions(), equalTo(1));
            assertThat(bulkRequest.requests().stream().findFirst().get(), equalTo(indexRequest));
            return true;
        }), any());
    }
}
