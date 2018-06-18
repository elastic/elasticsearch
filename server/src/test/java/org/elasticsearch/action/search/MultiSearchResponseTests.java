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
package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchResponseTests extends AbstractXContentTestCase<MultiSearchResponse> {

    @Override
    protected MultiSearchResponse createTestInstance() {
        int numItems = randomIntBetween(0, 128);
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            if (randomBoolean()) {
                // Creating a minimal response is OK, because SearchResponse self
                // is tested elsewhere.
                long tookInMillis = randomNonNegativeLong();
                int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
                int successfulShards = randomIntBetween(0, totalShards);
                int skippedShards = totalShards - successfulShards;
                InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
                SearchResponse.Clusters clusters = new SearchResponse.Clusters(totalShards, successfulShards, skippedShards);
                SearchResponse searchResponse = new SearchResponse(internalSearchResponse, null, totalShards,
                        successfulShards, skippedShards, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, clusters);
                items[i] = new MultiSearchResponse.Item(searchResponse, null);
            } else {
                items[i] = new MultiSearchResponse.Item(null, new ElasticsearchException("an error"));
            }
        }
        return new MultiSearchResponse(items, randomNonNegativeLong());
    }

    @Override
    protected MultiSearchResponse doParseInstance(XContentParser parser) throws IOException {
        return MultiSearchResponse.fromXContext(parser);
    }
    
    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }
    
    @Override
    protected void assertEqualInstances(MultiSearchResponse expected, MultiSearchResponse actual) {
        assertThat(actual.getTook(), equalTo(expected.getTook()));
        assertThat(actual.getResponses().length, equalTo(expected.getResponses().length));
        for (int i = 0; i < expected.getResponses().length; i++) {
            MultiSearchResponse.Item expectedItem = expected.getResponses()[i];
            MultiSearchResponse.Item actualItem = actual.getResponses()[i];
            if (expectedItem.isFailure()) {
                assertThat(actualItem.getResponse(), nullValue());
                assertThat(actualItem.getFailureMessage(), containsString(expectedItem.getFailureMessage()));
            } else {
                assertThat(actualItem.getResponse().toString(), equalTo(expectedItem.getResponse().toString()));
                assertThat(actualItem.getFailure(), nullValue());
            }
        }
    }

}
