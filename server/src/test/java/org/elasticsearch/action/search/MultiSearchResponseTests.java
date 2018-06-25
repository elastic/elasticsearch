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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < 20; runs++) {
            MultiSearchResponse expected = createTestInstance();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffled = toShuffledXContent(expected, xContentType, ToXContent.EMPTY_PARAMS, false);
            MultiSearchResponse actual;
            try (XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled)) {
                actual = MultiSearchResponse.fromXContext(parser);
                assertThat(parser.nextToken(), nullValue());
            }

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

    private static MultiSearchResponse createTestInstance() {
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

}
