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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BulkRequestModifierTests extends ESTestCase {

    public void testPipelineFailures() {
        BulkRequest originalBulkRequest = new BulkRequest();
        for (int i = 0; i < 32; i++) {
            originalBulkRequest.add(new IndexRequest("index", "type", String.valueOf(i)));
        }

        IngestActionFilter.BulkRequestModifier modifier = new IngestActionFilter.BulkRequestModifier(originalBulkRequest);
        for (int i = 0; modifier.hasNext(); i++) {
            modifier.next();
            if (i % 2 == 0) {
                modifier.markCurrentItemAsFailed(new RuntimeException());
            }
        }

        // So half of the requests have "failed", so only the successful requests are left:
        BulkRequest bulkRequest = modifier.getBulkRequest();
        assertThat(bulkRequest.requests().size(), Matchers.equalTo(16));

        List<BulkItemResponse> responses = new ArrayList<>();
        ActionListener<BulkResponse> bulkResponseListener = modifier.wrapActionListenerIfNeeded(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                responses.addAll(Arrays.asList(bulkItemResponses.getItems()));
            }

            @Override
            public void onFailure(Throwable e) {
            }
        });

        List<BulkItemResponse> originalResponses = new ArrayList<>();
        for (ActionRequest actionRequest : bulkRequest.requests()) {
            IndexRequest indexRequest = (IndexRequest) actionRequest;
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", 0), indexRequest.type(), indexRequest.id(), 1, true);
            originalResponses.add(new BulkItemResponse(Integer.parseInt(indexRequest.id()), indexRequest.opType().lowercase(), indexResponse));
        }
        bulkResponseListener.onResponse(new BulkResponse(originalResponses.toArray(new BulkItemResponse[0]), 0));

        assertThat(responses.size(), Matchers.equalTo(32));
        for (int i = 0; i < 32; i++) {
            assertThat(responses.get(i).getId(), Matchers.equalTo(String.valueOf(i)));
        }
    }

    public void testNoFailures() {
        BulkRequest originalBulkRequest = new BulkRequest();
        for (int i = 0; i < 32; i++) {
            originalBulkRequest.add(new IndexRequest("index", "type", String.valueOf(i)));
        }

        IngestActionFilter.BulkRequestModifier modifier = new IngestActionFilter.BulkRequestModifier(originalBulkRequest);
        for (int i = 0; modifier.hasNext(); i++) {
            modifier.next();
        }

        BulkRequest bulkRequest = modifier.getBulkRequest();
        assertThat(bulkRequest, Matchers.sameInstance(originalBulkRequest));
        ActionListener<BulkResponse> actionListener = Mockito.mock(ActionListener.class);
        assertThat(modifier.wrapActionListenerIfNeeded(actionListener), Matchers.sameInstance(actionListener));
    }

}
