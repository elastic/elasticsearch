/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;

public class AnalyticsCollectionServiceTests extends AnalyticsTestCase {
    private static final int NUM_COLLECTIONS = 10;

    @Before
    public void setup() throws Exception {
        for (int i = 0; i < NUM_COLLECTIONS; i++) {
            createDataStream(new AnalyticsCollection("collection_" + i).getEventDataStream());
        }
    }

    public void testGetExistingAnalyticsCollection() throws Exception {
        AnalyticsCollection analyticsCollection = awaitGetAnalyticsCollection("collection_1");
        assertThat(analyticsCollection.getName(), equalTo("collection_1"));
    }

    private void createDataStream(String dataStreamName) throws ExecutionException, InterruptedException {
        client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get();
    }

    private AnalyticsCollection awaitGetAnalyticsCollection(String collectionName) throws Exception {
        return new ResponseAwaiter<String, AnalyticsCollection>(analyticsCollectionService()::getAnalyticsCollection).get(collectionName);
    }

    private static class ResponseAwaiter<T, R> {
        BiConsumer<T, ActionListener<R>> f;

        public ResponseAwaiter(BiConsumer<T, ActionListener<R>> f) {
            this.f = f;
        }

        public R get(T param) throws Exception {
            CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<R> resp = new AtomicReference<>(null);
            final AtomicReference<Exception> exc = new AtomicReference<>(null);

            f.accept(param, new ActionListener<R>() {
                @Override
                public void onResponse(R r) {
                    resp.set(r);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exc.set(e);
                    latch.countDown();
                }
            });

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            if (exc.get() != null) {
                throw exc.get();
            }
            assertNotNull(resp.get());
            return resp.get();
        }
    }
}
