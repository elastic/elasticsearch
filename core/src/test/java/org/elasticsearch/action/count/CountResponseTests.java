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

package org.elasticsearch.action.count;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

public class CountResponseTests extends ESTestCase {

    @Test
    public void testFromSearchResponse() {
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(new InternalSearchHits(null, randomLong(), randomFloat()), null, null, randomBoolean(), randomBoolean());
        ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[randomIntBetween(0, 5)];
        for (int i = 0; i < shardSearchFailures.length; i++) {
            shardSearchFailures[i] = new ShardSearchFailure(new IllegalArgumentException());
        }
        SearchResponse searchResponse = new SearchResponse(internalSearchResponse, null, randomIntBetween(0, 100), randomIntBetween(0, 100), randomIntBetween(0, 100), shardSearchFailures);

        CountResponse countResponse = new CountResponse(searchResponse);
        assertThat(countResponse.getTotalShards(), equalTo(searchResponse.getTotalShards()));
        assertThat(countResponse.getSuccessfulShards(), equalTo(searchResponse.getSuccessfulShards()));
        assertThat(countResponse.getFailedShards(), equalTo(searchResponse.getFailedShards()));
        assertThat(countResponse.getShardFailures(), equalTo((ShardOperationFailedException[])searchResponse.getShardFailures()));
        assertThat(countResponse.getCount(), equalTo(searchResponse.getHits().totalHits()));
        assertThat(countResponse.terminatedEarly(), equalTo(searchResponse.isTerminatedEarly()));
    }
}
