/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BulkRequestModifierTests extends ESTestCase {

    public void testBulkRequestModifier() {
        int numRequests = scaledRandomIntBetween(8, 64);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numRequests; i++) {
            bulkRequest.add(new IndexRequest("_index").id(String.valueOf(i)).source("{}", XContentType.JSON));
        }
        CaptureActionListener actionListener = new CaptureActionListener();
        TransportBulkAction.BulkRequestModifier bulkRequestModifier = new TransportBulkAction.BulkRequestModifier(bulkRequest);

        int i = 0;
        Set<Integer> failedSlots = new HashSet<>();
        while (bulkRequestModifier.hasNext()) {
            bulkRequestModifier.next();
            if (randomBoolean()) {
                bulkRequestModifier.markItemAsFailed(i, new RuntimeException());
                failedSlots.add(i);
            }
            i++;
        }

        assertThat(bulkRequestModifier.getBulkRequest().requests().size(), equalTo(numRequests - failedSlots.size()));
        // simulate that we actually executed the modified bulk request:
        long ingestTook = randomLong();
        ActionListener<BulkResponse> result = bulkRequestModifier.wrapActionListenerIfNeeded(ingestTook, actionListener);
        result.onResponse(new BulkResponse(new BulkItemResponse[numRequests - failedSlots.size()], 0));

        BulkResponse bulkResponse = actionListener.getResponse();
        assertThat(bulkResponse.getIngestTookInMillis(), equalTo(ingestTook));
        for (int j = 0; j < bulkResponse.getItems().length; j++) {
            if (failedSlots.contains(j)) {
                BulkItemResponse item = bulkResponse.getItems()[j];
                assertThat(item.isFailed(), is(true));
                assertThat(item.getFailure().getIndex(), equalTo("_index"));
                assertThat(item.getFailure().getId(), equalTo(String.valueOf(j)));
                assertThat(item.getFailure().getMessage(), equalTo("java.lang.RuntimeException"));
            } else {
                assertThat(bulkResponse.getItems()[j], nullValue());
            }
        }
    }

    public void testPipelineFailures() {
        BulkRequest originalBulkRequest = new BulkRequest();
        for (int i = 0; i < 32; i++) {
            originalBulkRequest.add(new IndexRequest("index").id(String.valueOf(i)));
        }

        TransportBulkAction.BulkRequestModifier modifier = new TransportBulkAction.BulkRequestModifier(originalBulkRequest);
        for (int i = 0; modifier.hasNext(); i++) {
            modifier.next();
            if (i % 2 == 0) {
                modifier.markItemAsFailed(i, new RuntimeException());
            }
        }

        // So half of the requests have "failed", so only the successful requests are left:
        BulkRequest bulkRequest = modifier.getBulkRequest();
        assertThat(bulkRequest.requests().size(), Matchers.equalTo(16));

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
        bulkResponseListener.onResponse(new BulkResponse(originalResponses.toArray(new BulkItemResponse[originalResponses.size()]), 0));

        assertThat(responses.size(), Matchers.equalTo(32));
        for (int i = 0; i < 32; i++) {
            assertThat(responses.get(i).getId(), Matchers.equalTo(String.valueOf(i)));
        }
    }

    public void testNoFailures() {
        BulkRequest originalBulkRequest = new BulkRequest();
        for (int i = 0; i < 32; i++) {
            originalBulkRequest.add(new IndexRequest("index").id(String.valueOf(i)));
        }

        TransportBulkAction.BulkRequestModifier modifier = new TransportBulkAction.BulkRequestModifier(originalBulkRequest);
        while (modifier.hasNext()) {
            modifier.next();
        }

        BulkRequest bulkRequest = modifier.getBulkRequest();
        assertThat(bulkRequest, Matchers.sameInstance(originalBulkRequest));
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
