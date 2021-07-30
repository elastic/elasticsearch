/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.eql;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class EqlStatsResponseTests extends AbstractResponseTestCase<EqlStatsResponseToXContent, EqlStatsResponse> {

    private static Map<String, Object> buildRandomCountersMap(int count) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < count; i++) {
            map.put(randomAlphaOfLength(10), randomIntBetween(0, Integer.MAX_VALUE));
        }
        return map;
    }

    private static Map<String, Object> buildRandomNodeStats(int featuresNumber) {
        Map<String, Object> stats = new HashMap<>();

        int countersNumber = randomIntBetween(0, 10);

        Map<String, Object> features = new HashMap<>();
        for (int i = 0; i < featuresNumber; i++) {
            features.put(randomAlphaOfLength(10), buildRandomCountersMap(countersNumber));
        }

        stats.put("features", features);

        Map<String, Object> res = new HashMap<>();
        res.put("stats", stats);
        return res;
    }

    @Override
    protected EqlStatsResponseToXContent createServerTestInstance(XContentType xContentType) {
        NodesResponseHeader header = new NodesResponseHeader(randomInt(10), randomInt(10),
                randomInt(10), Collections.emptyList());
        String clusterName = randomAlphaOfLength(10);

        int nodeCount = randomInt(10);
        int featuresNumber = randomIntBetween(0, 10);
        List<EqlStatsResponse.Node> nodes = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            Map<String, Object> stat = buildRandomNodeStats(featuresNumber);
            nodes.add(new EqlStatsResponse.Node(stat));
        }
        EqlStatsResponse response = new EqlStatsResponse(header, clusterName, nodes);
        return new EqlStatsResponseToXContent(new EqlStatsResponse(header, clusterName, nodes));
    }

    @Override
    protected EqlStatsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return EqlStatsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(EqlStatsResponseToXContent serverTestInstanceWrap, EqlStatsResponse clientInstance) {
        EqlStatsResponse serverTestInstance = serverTestInstanceWrap.unwrap();
        assertThat(serverTestInstance, is(clientInstance));
    }
}
