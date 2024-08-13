/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class IncrementalBulkIT extends ESIntegTestCase {

    public void testSingleBulkRequest() {
        String index = "test";
        createIndex(index);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
        IndexRequest indexRequest = indexRequest(index);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        handler.lastItems(List.of(indexRequest), refCounted::decRef, future);

        BulkResponse bulkResponse = future.actionGet();
        assertNoFailures(bulkResponse);

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) 1));
        });

        assertFalse(refCounted.hasReferences());
    }

    public void testMultipleBulkPartsWithBackoff() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try (Releasable ignored = executorService::shutdown;) {
            String index = "test";
            createIndex(index);

            IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);
            AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
            AtomicLong hits = new AtomicLong(0);

            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
            int docs = randomIntBetween(200, 400);
            ConcurrentLinkedQueue<DocWriteRequest<?>> queue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < docs; i++) {
                hits.incrementAndGet();
                IndexRequest indexRequest = indexRequest(index);
                queue.add(indexRequest);
            }

            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
            Runnable r = new Runnable() {

                @Override
                public void run() {
                    int toRemove = Math.min(randomIntBetween(5, 10), queue.size());
                    ArrayList<DocWriteRequest<?>> docs = new ArrayList<>();
                    for (int i = 0; i < toRemove; i++) {
                        docs.add(queue.poll());
                    }

                    if (queue.isEmpty()) {
                        handler.lastItems(docs, refCounted::decRef, future);
                    } else {
                        refCounted.incRef();
                        handler.addItems(docs, refCounted::decRef, () -> executorService.execute(this));
                    }
                }
            };

            executorService.execute(r);

            BulkResponse bulkResponse = future.actionGet();
            assertNoFailures(bulkResponse);

            refresh(index);

            assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(hits.get()));
            });

            assertFalse(refCounted.hasReferences());
        }
    }

    private static IndexRequest indexRequest(String index) {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.source(Map.of("field", randomAlphaOfLength(10)));
        return indexRequest;
    }
}
