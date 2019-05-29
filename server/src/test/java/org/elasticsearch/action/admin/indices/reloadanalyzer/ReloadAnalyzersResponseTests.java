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

package org.elasticsearch.action.admin.indices.reloadanalyzer;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.AbstractBroadcastResponseTestCase;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReloadAnalyzersResponseTests extends AbstractBroadcastResponseTestCase<ReloadAnalyzersResponse> {

    @Override
    protected ReloadAnalyzersResponse createTestInstance(int totalShards, int successfulShards, int failedShards,
                                                 List<DefaultShardOperationFailedException> failures) {
        Map<String, List<String>> reloadedIndicesNodes = new HashMap<>();
        int randomIndices = randomIntBetween(0, 5);
        for (int i = 0; i < randomIndices; i++) {
            List<String> randomNodeIds = Arrays.asList(generateRandomStringArray(5, 5, false, true));
            reloadedIndicesNodes.put(randomAlphaOfLengthBetween(5, 10), randomNodeIds);
        }
        return new ReloadAnalyzersResponse(totalShards, successfulShards, failedShards, failures, reloadedIndicesNodes);
    }

    @Override
    protected ReloadAnalyzersResponse doParseInstance(XContentParser parser) throws IOException {
        return ReloadAnalyzersResponse.fromXContent(parser);
    }

    @Override
    public void testToXContent() {
        Map<String, List<String>> reloadedIndicesNodes = Collections.singletonMap("index", Collections.singletonList("nodeId"));
        ReloadAnalyzersResponse response = new ReloadAnalyzersResponse(10, 5, 5, null, reloadedIndicesNodes);
        String output = Strings.toString(response);
        assertEquals(
                "{\"_shards\":{\"total\":10,\"successful\":5,\"failed\":5},"
                + "\"reloaded_nodes\":[{\"index\":\"index\",\"reloaded_node_ids\":[\"nodeId\"]}]"
                + "}",
                output);
    }
}
