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
