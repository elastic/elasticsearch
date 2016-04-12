package org.elasticsearch.action.ingest;

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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class BulkRequestModifierTests extends ESTestCase {

    private final static ShardId SHARD_ID = new ShardId("_index", "_indexUUID", 0);

    public void testBulkRequestModifier() {
        int numRequests = scaledRandomIntBetween(8, 64);
        List<BulkItemRequest> items = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            items.add(new BulkItemRequest(i, new IndexRequest("_index", "_type", String.valueOf(i)).source("{}")));
        }
        BulkShardRequest bulkRequest = new BulkShardRequest(SHARD_ID, false, items.toArray(new BulkItemRequest[0]));
        CaptureActionListener actionListener = new CaptureActionListener();
        IngestActionFilter.BulkRequestModifier bulkRequestModifier = new IngestActionFilter.BulkRequestModifier(bulkRequest);

        Set<Integer> failedSlots = new HashSet<>();
        while (bulkRequestModifier.hasNext()) {
            bulkRequestModifier.next();
            if (randomBoolean()) {
                bulkRequestModifier.markCurrentItemAsFailed(new RuntimeException());
                failedSlots.add(bulkRequestModifier.current.id());
            }
        }

        assertThat(bulkRequestModifier.getBulkShardRequest().items().length, equalTo(numRequests - failedSlots.size()));
        // simulate that we actually executed the modified bulk request:
        long ingestTook = randomLong();

        ActionListener<BulkShardResponse> result = bulkRequestModifier.wrapActionListenerIfNeeded(ingestTook, actionListener);
        List<BulkItemResponse> responses = new ArrayList<>(numRequests - failedSlots.size());
        for (int j = 0; j < items.size(); j++) {
            int id = items.get(j).id();
            if (failedSlots.contains(id)) {
                continue;
            }
            responses.add(new BulkItemResponse(id, "nothing", new IndexResponse()));
        }
        result.onResponse(new BulkShardResponse(new BulkShardResponse(), responses.toArray(new BulkItemResponse[responses.size()]), 0));

        BulkShardResponse bulkResponse = actionListener.getResponse();
        assertThat(bulkResponse.getIngestTookInMillis(), equalTo(ingestTook));
        for (BulkItemResponse item : bulkResponse.getResponses()) {
            if (failedSlots.contains(item.getItemId())) {
                assertThat(item.isFailed(), is(true));
                assertThat(item.getFailure().getIndex(), equalTo("_index"));
                assertThat(item.getFailure().getType(), equalTo("_type"));
                assertThat(item.getFailure().getMessage(), equalTo("java.lang.RuntimeException"));
            } else {
                assertThat(item.getOpType(), sameInstance("nothing"));
            }
        }
    }

    public void testPipelineFailures() {
        List<BulkItemRequest> items = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            items.add(new BulkItemRequest(i, new IndexRequest("index", "type", String.valueOf(i))));
        }
        BulkShardRequest originalBulkRequest =
                new BulkShardRequest(SHARD_ID, false, items.toArray(new BulkItemRequest[0]));

        IngestActionFilter.BulkRequestModifier modifier = new IngestActionFilter.BulkRequestModifier(originalBulkRequest);
        for (int i = 0; modifier.hasNext(); i++) {
            modifier.next();
            if (i % 2 == 0) {
                modifier.markCurrentItemAsFailed(new RuntimeException());
            }
        }

        // So half of the requests have "failed", so only the successful requests are left:
        BulkShardRequest bulkRequest = modifier.getBulkShardRequest();
        assertThat(bulkRequest.items().length, Matchers.equalTo(16));

        List<BulkItemResponse> responses = new ArrayList<>();
        ActionListener<BulkShardResponse> bulkResponseListener = modifier.wrapActionListenerIfNeeded(1L, new ActionListener<BulkShardResponse>() {
            @Override
            public void onResponse(BulkShardResponse bulkItemResponses) {
                responses.addAll(Arrays.asList(bulkItemResponses.getResponses()));
            }

            @Override
            public void onFailure(Throwable e) {
            }
        });

        List<BulkItemResponse> originalResponses = new ArrayList<>();
        for (BulkItemRequest item : bulkRequest.items()) {
            ActionRequest actionRequest = item.request();
            IndexRequest indexRequest = (IndexRequest) actionRequest;
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "_na_", 0), indexRequest.type(), indexRequest.id(), 1, true);
            originalResponses.add(new BulkItemResponse(Integer.parseInt(indexRequest.id()), indexRequest.opType().lowercase(), indexResponse));
        }
        bulkResponseListener.onResponse(
                new BulkShardResponse(new BulkShardResponse(),
                        originalResponses.toArray(new BulkItemResponse[originalResponses.size()]), 0)
        );

        assertThat(responses.size(), Matchers.equalTo(32));
        for (int i = 0; i < 32; i++) {
            assertThat(responses.get(i).getId(), Matchers.equalTo(String.valueOf(i)));
        }
    }

    public void testNoFailures() {
        BulkItemRequest[] items = new BulkItemRequest[32];
        for (int i = 0; i < items.length; i++) {
            items[i] = new BulkItemRequest(i, new IndexRequest("index", "type", String.valueOf(i)));
        }
        BulkShardRequest originalBulkRequest = new BulkShardRequest(SHARD_ID, false, items);

        IngestActionFilter.BulkRequestModifier modifier = new IngestActionFilter.BulkRequestModifier(originalBulkRequest);
        while (modifier.hasNext()) {
            modifier.next();
        }

        BulkShardRequest bulkRequest = modifier.getBulkShardRequest();
        assertThat(bulkRequest.items(), Matchers.equalTo(originalBulkRequest.items()));
    }

    private static class CaptureActionListener implements ActionListener<BulkShardResponse> {

        private BulkShardResponse response;

        @Override
        public void onResponse(BulkShardResponse bulkItemResponses) {
            this.response = bulkItemResponses;
        }

        @Override
        public void onFailure(Throwable e) {
        }

        public BulkShardResponse getResponse() {
            return response;
        }
    }
}
