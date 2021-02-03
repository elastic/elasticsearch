/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse.ReloadDetails;

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
    protected ReloadAnalyzersResponse createTestInstance(int totalShards, int successfulShards, int failedShards,
                                                 List<DefaultShardOperationFailedException> failures) {
        Map<String, ReloadDetails> reloadedIndicesDetails = new HashMap<>();
        int randomIndices = randomIntBetween(0, 5);
        for (int i = 0; i < randomIndices; i++) {
            String name = randomAlphaOfLengthBetween(5, 10);
            Set<String> reloadedIndicesNodes = new HashSet<>(Arrays.asList(generateRandomStringArray(5, 5, false, true)));
            Set<String> reloadedAnalyzers = new HashSet<>(Arrays.asList(generateRandomStringArray(5, 5, false, true)));
            reloadedIndicesDetails.put(name, new ReloadDetails(name, reloadedIndicesNodes, reloadedAnalyzers));
        }
        return new ReloadAnalyzersResponse(totalShards, successfulShards, failedShards, failures, reloadedIndicesDetails);
    }

    @Override
    protected ReloadAnalyzersResponse doParseInstance(XContentParser parser) throws IOException {
        return ReloadAnalyzersResponse.fromXContent(parser);
    }

    @Override
    public void testToXContent() {
        Map<String, ReloadDetails> reloadedIndicesNodes = Collections.singletonMap("index",
                new ReloadDetails("index", Collections.singleton("nodeId"), Collections.singleton("my_analyzer")));
        ReloadAnalyzersResponse response = new ReloadAnalyzersResponse(10, 5, 5, null, reloadedIndicesNodes);
        String output = Strings.toString(response);
        assertEquals(
                "{\"_shards\":{\"total\":10,\"successful\":5,\"failed\":5},"
                + "\"reload_details\":[{\"index\":\"index\",\"reloaded_analyzers\":[\"my_analyzer\"],\"reloaded_node_ids\":[\"nodeId\"]}]"
                + "}",
                output);
    }

    public void testSerialization() throws IOException {
        ReloadAnalyzersResponse response = createTestInstance();
        ReloadAnalyzersResponse copy = copyWriteable(response, writableRegistry(), ReloadAnalyzersResponse::new,
                VersionUtils.randomVersion(random()));
        assertEquals(response.getReloadDetails(), copy.getReloadDetails());
    }

}
