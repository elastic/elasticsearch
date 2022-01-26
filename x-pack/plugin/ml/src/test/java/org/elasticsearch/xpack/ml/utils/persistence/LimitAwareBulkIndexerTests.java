/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LimitAwareBulkIndexerTests extends ESTestCase {

    private List<BulkRequest> executedBulkRequests = new ArrayList<>();

    public void testAddAndExecuteIfNeeded_GivenRequestsReachingBytesLimit() {
        try (LimitAwareBulkIndexer bulkIndexer = createIndexer(100)) {
            bulkIndexer.addAndExecuteIfNeeded(mockIndexRequest(50));
            assertThat(executedBulkRequests, is(empty()));

            bulkIndexer.addAndExecuteIfNeeded(mockIndexRequest(50));
            assertThat(executedBulkRequests, is(empty()));

            bulkIndexer.addAndExecuteIfNeeded(mockIndexRequest(50));
            assertThat(executedBulkRequests, hasSize(1));
            assertThat(executedBulkRequests.get(0).numberOfActions(), equalTo(2));

            bulkIndexer.addAndExecuteIfNeeded(mockIndexRequest(50));
            assertThat(executedBulkRequests, hasSize(1));

            bulkIndexer.addAndExecuteIfNeeded(mockIndexRequest(50));
            assertThat(executedBulkRequests, hasSize(2));
            assertThat(executedBulkRequests.get(1).numberOfActions(), equalTo(2));
        }

        assertThat(executedBulkRequests, hasSize(3));
        assertThat(executedBulkRequests.get(2).numberOfActions(), equalTo(1));
    }

    public void testAddAndExecuteIfNeeded_GivenRequestsReachingBatchSize() {
        try (LimitAwareBulkIndexer bulkIndexer = createIndexer(10000)) {
            for (int i = 0; i < 1000; i++) {
                bulkIndexer.addAndExecuteIfNeeded(mockIndexRequest(1));
            }
            assertThat(executedBulkRequests, is(empty()));

            bulkIndexer.addAndExecuteIfNeeded(mockIndexRequest(1));

            assertThat(executedBulkRequests, hasSize(1));
            assertThat(executedBulkRequests.get(0).numberOfActions(), equalTo(1000));
        }

        assertThat(executedBulkRequests, hasSize(2));
        assertThat(executedBulkRequests.get(1).numberOfActions(), equalTo(1));
    }

    public void testNoRequests() {
        try (LimitAwareBulkIndexer bulkIndexer = createIndexer(10000)) {}

        assertThat(executedBulkRequests, is(empty()));
    }

    private LimitAwareBulkIndexer createIndexer(long bytesLimit) {
        return new LimitAwareBulkIndexer(bytesLimit, executedBulkRequests::add);
    }

    private static IndexRequest mockIndexRequest(long ramBytes) {
        IndexRequest indexRequest = mock(IndexRequest.class);
        when(indexRequest.ramBytesUsed()).thenReturn(ramBytes);
        return indexRequest;
    }
}
