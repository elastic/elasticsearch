/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReloadAnalyzersResponseTests extends AbstractBroadcastResponseTestCase<ReloadAnalyzersResponse> {

    @Override
    protected ReloadAnalyzersResponse createTestInstance(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> failures
    ) {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesDetails = createRandomReloadDetails();
        return new ReloadAnalyzersResponse(totalShards, successfulShards, failedShards, failures, reloadedIndicesDetails);
    }

    public static Map<String, ReloadAnalyzersResponse.ReloadDetails> createRandomReloadDetails() {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesDetails = new HashMap<>();
        int randomIndices = randomIntBetween(0, 5);
        for (int i = 0; i < randomIndices; i++) {
            String name = randomAlphaOfLengthBetween(5, 10);
            Set<String> reloadedIndicesNodes = new HashSet<>(Arrays.asList(generateRandomStringArray(5, 5, false, true)));
            Set<String> reloadedAnalyzers = new HashSet<>(Arrays.asList(generateRandomStringArray(5, 5, false, true)));
            reloadedIndicesDetails.put(name, new ReloadAnalyzersResponse.ReloadDetails(name, reloadedIndicesNodes, reloadedAnalyzers));
        }
        return reloadedIndicesDetails;
    }

    @Override
    protected ReloadAnalyzersResponse doParseInstance(XContentParser parser) throws IOException {
        return ReloadAnalyzersResponse.fromXContent(parser);
    }

    @Override
    public void testToXContent() throws IOException {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesNodes = Collections.singletonMap(
            "index",
            new ReloadAnalyzersResponse.ReloadDetails("index", Collections.singleton("nodeId"), Collections.singleton("my_analyzer"))
        );
        ReloadAnalyzersResponse response = new ReloadAnalyzersResponse(10, 5, 5, null, reloadedIndicesNodes);
        String output = Strings.toString(response);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_shards": {
                "total": 10,
                "successful": 5,
                "failed": 5
              },
              "reload_details": [
                {
                  "index": "index",
                  "reloaded_analyzers": [ "my_analyzer" ],
                  "reloaded_node_ids": [ "nodeId" ]
                }
              ]
            }"""), output);
    }

    public void testSerialization() throws IOException {
        ReloadAnalyzersResponse response = createTestInstance();
        ReloadAnalyzersResponse copy = copyWriteable(
            response,
            writableRegistry(),
            ReloadAnalyzersResponse::new,
            TransportVersionUtils.randomVersion(random())
        );
        assertEquals(response.getReloadDetails(), copy.getReloadDetails());
    }

}
