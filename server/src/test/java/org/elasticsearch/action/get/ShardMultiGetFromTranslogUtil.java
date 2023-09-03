/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.index.shard.ShardId;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ShardMultiGetFromTranslogUtil {

    // Converts a MultiGetRequest that has IDs of only one shard to a MultiGetShardRequest
    public static MultiGetShardRequest newMultiGetShardRequest(MultiGetRequest multiGetRequest, ShardId shardId) {
        var request = new MultiGetShardRequest(multiGetRequest, shardId.getIndexName(), shardId.id());
        for (int i = 0; i < multiGetRequest.items.size(); i++) {
            request.add(i, multiGetRequest.items.get(i));
        }
        assertThat(multiGetRequest.getItems().size(), equalTo(request.items.size()));
        return request;
    }

    public static List<Integer> getLocations(MultiGetShardResponse response) {
        return response.locations;
    }

    public static List<GetResponse> getResponses(MultiGetShardResponse response) {
        return response.responses;
    }

    public static List<MultiGetResponse.Failure> getFailures(MultiGetShardResponse response) {
        return response.failures;
    }
}
