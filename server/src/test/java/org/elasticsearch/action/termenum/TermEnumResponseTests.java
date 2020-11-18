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

package org.elasticsearch.action.termenum;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TermEnumResponseTests extends AbstractBroadcastResponseTestCase<TermEnumResponse> {

    protected static List<TermCount> getRandomTerms() {
        int termCount = randomIntBetween(0, 100);
        Map<String, TermCount> uniqueTerms = new HashMap<>(termCount);
        while (uniqueTerms.size() < termCount) {
            String s = randomAlphaOfLengthBetween(1, 10);
            uniqueTerms.put(s, new TermCount(s, randomIntBetween(1, 100)));
        }
        List<TermCount> terms = new ArrayList<>(uniqueTerms.values());
        return terms;
    }

    private static TermEnumResponse createRandomTermEnumResponse() {
        int totalShards = randomIntBetween(1, 10);
        int successfulShards = randomIntBetween(0, totalShards);
        int failedShards = totalShards - successfulShards;
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>(failedShards);
        for (int i=0; i<failedShards; i++) {
            ElasticsearchException exc = new ElasticsearchException("some_error_" + randomInt());
            String index = "index_" + randomInt(1000);
            int shard = randomInt(100);
            shardFailures.add(
                new DefaultShardOperationFailedException(index, shard, exc)
            );
        }
        return new TermEnumResponse(getRandomTerms(), totalShards, successfulShards, failedShards, shardFailures, randomBoolean());
    }

    @Override
    protected TermEnumResponse doParseInstance(XContentParser parser) throws IOException {
        return TermEnumResponse.fromXContent(parser);
    }

    @Override
    protected TermEnumResponse createTestInstance() {
        return createRandomTermEnumResponse();
    }

    @Override
    protected void assertEqualInstances(TermEnumResponse response, TermEnumResponse parsedResponse) {
        super.assertEqualInstances(response, parsedResponse);
        Set<TermCount> terms = new HashSet<>(response.getTerms());
        assertEquals(response.getTerms().size(), parsedResponse.getTerms().size());
        assertTrue(terms.containsAll(parsedResponse.getTerms()));
    }

    @Override
    protected TermEnumResponse createTestInstance(int totalShards, int successfulShards, int failedShards,
                                                       List<DefaultShardOperationFailedException> failures) {
        return new TermEnumResponse(getRandomTerms(), totalShards, successfulShards, failedShards, failures, randomBoolean());

    }

    @Override
    public void testToXContent() {
        TermCount tc = new TermCount(randomAlphaOfLengthBetween(1, 10), randomIntBetween(1,  Integer.MAX_VALUE));
        List<TermCount> terms = new ArrayList<>();
        terms.add(tc);
        TermEnumResponse response = new TermEnumResponse(terms, 10, 10, 0, new ArrayList<>(), true);

        String output = Strings.toString(response);
        assertEquals("{\"_shards\":{\"total\":10,\"successful\":10,\"failed\":0},\"terms\":[{" +
            "\"term\":\""+tc.getTerm()+"\","+
            "\"doc_count\":"+tc.getDocCount()+
            "}],\"timed_out\":true}", output);
    }
}
