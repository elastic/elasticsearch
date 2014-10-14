/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bench;

import org.elasticsearch.action.bench.BenchmarkExecutor.StoppableSemaphore;
import org.elasticsearch.action.search.SearchResponse;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class StatisticCollectionActionListener extends BoundsManagingActionListener<SearchResponse> {

    private final Object[] statsBuckets;
    private final int bucketId;
    private final long[] docBuckets;
    private Evaluator evaluator;

    public StatisticCollectionActionListener(StoppableSemaphore semaphore, Object[] statsBuckets, long[] docs,
                                             int bucketId, Evaluator evaluator, 
                                             CountDownLatch totalCount,
                                             CopyOnWriteArrayList<String> errorMessages) {
        super(semaphore, totalCount, errorMessages);
        this.bucketId = bucketId;
        this.statsBuckets = statsBuckets;
        this.docBuckets = docs;
        this.evaluator = evaluator;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        super.onResponse(searchResponse);
        if (evaluator == null) {
            statsBuckets[bucketId] = searchResponse.getTookInMillis();
        } else {
            statsBuckets[bucketId] = evaluator.evaluate(searchResponse.getHits().getHits());
        }
        if (searchResponse.getHits() != null) {
            docBuckets[bucketId] = searchResponse.getHits().getTotalHits();
        }
    }

    @Override
    public void onFailure(Throwable e) {
        try {
            statsBuckets[bucketId] = -1;
            docBuckets[bucketId] = -1;
        } finally {
            super.onFailure(e);
        }

    }
}