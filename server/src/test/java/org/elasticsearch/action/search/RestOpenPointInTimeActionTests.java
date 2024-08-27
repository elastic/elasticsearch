/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class RestOpenPointInTimeActionTests extends RestActionTestCase {

    public void testMaxConcurrentSearchRequests() {
        RestOpenPointInTimeAction action = new RestOpenPointInTimeAction();
        controller().registerHandler(action);
        Queue<OpenPointInTimeRequest> transportRequests = ConcurrentCollections.newQueue();
        verifyingClient.setExecuteVerifier(((actionType, transportRequest) -> {
            assertThat(transportRequest, instanceOf(OpenPointInTimeRequest.class));
            transportRequests.add((OpenPointInTimeRequest) transportRequest);
            return new OpenPointInTimeResponse(new BytesArray("n/a"), 1, 1, 0, 0);
        }));
        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/_pit")
                .build();
            dispatchRequest(request);
            assertThat(transportRequests, hasSize(1));
            OpenPointInTimeRequest transportRequest = transportRequests.remove();
            assertThat(transportRequest.maxConcurrentShardRequests(), equalTo(5));
        }
        {
            int maxConcurrentRequests = randomIntBetween(1, 100);
            Map<String, String> params = new HashMap<>();
            params.put("max_concurrent_shard_requests", Integer.toString(maxConcurrentRequests));
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/_pit")
                .withParams(params)
                .build();
            dispatchRequest(request);
            assertThat(transportRequests, hasSize(1));
            OpenPointInTimeRequest transportRequest = transportRequests.remove();
            assertThat(transportRequest.maxConcurrentShardRequests(), equalTo(maxConcurrentRequests));
        }
    }
}
