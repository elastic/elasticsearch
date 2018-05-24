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
package org.elasticsearch.script.mustache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchTemplateResponseTests extends AbstractXContentTestCase<MultiSearchTemplateResponse> {

    @Override
    protected MultiSearchTemplateResponse createTestInstance() {
        int numItems = randomIntBetween(0, 128);
        long overallTookInMillis = randomNonNegativeLong();
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[numItems];
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
                SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
                SearchResponse searchResponse = new SearchResponse(internalSearchResponse, null, totalShards,
                        successfulShards, skippedShards, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, clusters);
                searchTemplateResponse.setResponse(searchResponse);
                items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
            } else {
                items[i] = new MultiSearchTemplateResponse.Item(null, new ElasticsearchException("an error"));
            }
        }
        return new MultiSearchTemplateResponse(items, overallTookInMillis);
    }

    @Override
    protected MultiSearchTemplateResponse doParseInstance(XContentParser parser) throws IOException {
        return MultiSearchTemplateResponse.fromXContext(parser);
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
    protected void assertEqualInstances(MultiSearchTemplateResponse expectedInstance, MultiSearchTemplateResponse newInstance) {
        assertThat(newInstance.getTook(), equalTo(expectedInstance.getTook()));
        assertThat(newInstance.getResponses().length, equalTo(expectedInstance.getResponses().length));
        for (int i = 0; i < expectedInstance.getResponses().length; i++) {
            MultiSearchTemplateResponse.Item expectedItem = expectedInstance.getResponses()[i];
            MultiSearchTemplateResponse.Item actualItem = newInstance.getResponses()[i];
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
