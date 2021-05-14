/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class ListenerActionIT extends ESIntegTestCase {
    public void testThreadedListeners() throws Throwable {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<String> threadName = new AtomicReference<>();
        Client client = client();

        IndexRequest request = new IndexRequest("test").id("1");
        if (randomBoolean()) {
            // set the source, without it, we will have a verification failure
            request.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
        }

        client.index(request, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                threadName.set(Thread.currentThread().getName());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                threadName.set(Thread.currentThread().getName());
                failure.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertFalse(threadName.get().contains("listener"));
    }
}
