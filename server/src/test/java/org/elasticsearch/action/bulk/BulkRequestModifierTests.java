/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerTests;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class BulkRequestModifierTests extends ESTestCase {

    public void testBulkRequestModifier() {
        int numRequests = scaledRandomIntBetween(8, 64);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numRequests; i++) {
            bulkRequest.add(new IndexRequest("_index").id(String.valueOf(i)).source("{}", XContentType.JSON));
        }

        // wrap the bulk request and fail some of the item requests at random
        BulkRequestModifier modifier = new BulkRequestModifier(bulkRequest);
        Set<Integer> failedSlots = new HashSet<>();
        for (int i = 0; modifier.hasNext(); i++) {
            modifier.next();
            if (randomBoolean()) {
                modifier.markItemAsFailed(i, new RuntimeException(), randomFrom(IndexDocFailureStoreStatus.values()));
                failedSlots.add(i);
            }
        }
        assertThat(modifier.getBulkRequest().requests().size(), equalTo(numRequests - failedSlots.size()));

        // populate the non-failed responses
        BulkRequest subsequentBulkRequest = modifier.getBulkRequest();
        assertThat(subsequentBulkRequest.requests().size(), equalTo(numRequests - failedSlots.size()));
        List<BulkItemResponse> responses = new ArrayList<>();
        for (int j = 0; j < subsequentBulkRequest.requests().size(); j++) {
            IndexRequest indexRequest = (IndexRequest) subsequentBulkRequest.requests().get(j);
            IndexResponse indexResponse = new IndexResponse(new ShardId("_index", "_na_", 0), indexRequest.id(), 1, 17, 1, true);
            responses.add(BulkItemResponse.success(j, indexRequest.opType(), indexResponse));
        }

        // simulate that we actually executed the modified bulk request
        long ingestTook = randomLong();
        CaptureActionListener actionListener = new CaptureActionListener();
        ActionListener<BulkResponse> result = modifier.wrapActionListenerIfNeeded(ingestTook, actionListener);
        result.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[0]), 0));

        // check the results for successes and failures
        BulkResponse bulkResponse = actionListener.getResponse();
        assertThat(bulkResponse.getIngestTookInMillis(), equalTo(ingestTook));
        for (int i = 0; i < bulkResponse.getItems().length; i++) {
            BulkItemResponse item = bulkResponse.getItems()[i];
            if (failedSlots.contains(i)) {
                assertThat(item.isFailed(), is(true));
                BulkItemResponse.Failure failure = item.getFailure();
                assertThat(failure.getIndex(), equalTo("_index"));
                assertThat(failure.getId(), equalTo(String.valueOf(i)));
                assertThat(failure.getMessage(), equalTo("java.lang.RuntimeException"));
            } else {
                assertThat(item.isFailed(), is(false));
                IndexResponse success = item.getResponse();
                assertThat(success.getIndex(), equalTo("_index"));
                assertThat(success.getId(), equalTo(String.valueOf(i)));
            }
        }
    }

    public void testPipelineFailures() {
        BulkRequest originalBulkRequest = new BulkRequest();
        for (int i = 0; i < 32; i++) {
            originalBulkRequest.add(new IndexRequest("index").id(String.valueOf(i)));
        }

        BulkRequestModifier modifier = new BulkRequestModifier(originalBulkRequest);

        final List<Integer> failures = new ArrayList<>();
        // iterate the requests in order, recording that half of them should be failures
        for (int i = 0; modifier.hasNext(); i++) {
            modifier.next();
            if (i % 2 == 0) {
                failures.add(i);
            }
        }

        // with async processors, the failures can come back 'out of order' so sometimes we'll shuffle the list
        if (randomBoolean()) {
            Collections.shuffle(failures, random());
        }

        // actually mark the failures
        for (int i : failures) {
            modifier.markItemAsFailed(i, new RuntimeException(), randomFrom(IndexDocFailureStoreStatus.values()));
        }

        // So half of the requests have "failed", so only the successful requests are left:
        BulkRequest bulkRequest = modifier.getBulkRequest();
        assertThat(bulkRequest.requests().size(), equalTo(16));

        List<BulkItemResponse> responses = new ArrayList<>();
        ActionListener<BulkResponse> bulkResponseListener = modifier.wrapActionListenerIfNeeded(1L, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                responses.addAll(Arrays.asList(bulkItemResponses.getItems()));
            }

            @Override
            public void onFailure(Exception e) {}
        });

        List<BulkItemResponse> originalResponses = new ArrayList<>();
        for (DocWriteRequest<?> actionRequest : bulkRequest.requests()) {
            IndexRequest indexRequest = (IndexRequest) actionRequest;
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "_na_", 0), indexRequest.id(), 1, 17, 1, true);
            originalResponses.add(BulkItemResponse.success(Integer.parseInt(indexRequest.id()), indexRequest.opType(), indexResponse));
        }
        bulkResponseListener.onResponse(new BulkResponse(originalResponses.toArray(new BulkItemResponse[0]), 0));

        assertThat(responses.size(), equalTo(32));
        for (int i = 0; i < 32; i++) {
            assertThat(responses.get(i).getId(), equalTo(String.valueOf(i)));
        }
    }

    public void testNoFailures() {
        BulkRequest originalBulkRequest = new BulkRequest();
        for (int i = 0; i < 32; i++) {
            originalBulkRequest.add(new IndexRequest("index").id(String.valueOf(i)));
        }

        BulkRequestModifier modifier = new BulkRequestModifier(originalBulkRequest);
        while (modifier.hasNext()) {
            modifier.next();
        }

        BulkRequest bulkRequest = modifier.getBulkRequest();
        assertThat(bulkRequest, sameInstance(originalBulkRequest));
        assertThat(modifier.wrapActionListenerIfNeeded(1L, ActionListener.noop()), ActionListenerTests.isMappedActionListener());
    }

    private static class CaptureActionListener implements ActionListener<BulkResponse> {

        private BulkResponse response;

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            this.response = bulkItemResponses;
        }

        @Override
        public void onFailure(Exception e) {}

        public BulkResponse getResponse() {
            return response;
        }
    }
}
