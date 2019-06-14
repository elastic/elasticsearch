/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class GetDatafeedStatsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<Response.DatafeedStats> datafeedStatsList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String datafeedId = randomAlphaOfLength(10);
            DatafeedState datafeedState = randomFrom(DatafeedState.values());

            DiscoveryNode node = null;
            if (randomBoolean()) {
                node = new DiscoveryNode("_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
            }
            String explanation = null;
            if (randomBoolean()) {
                explanation = randomAlphaOfLength(3);
            }
            Response.DatafeedStats datafeedStats = new Response.DatafeedStats(datafeedId, datafeedState, node, explanation);
            datafeedStatsList.add(datafeedStats);
        }

        result = new Response(new QueryPage<>(datafeedStatsList, datafeedStatsList.size(), DatafeedConfig.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

    @SuppressWarnings("unchecked")
    public void testDatafeedStatsToXContent() throws IOException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ml.enabled", "true");
        attributes.put("ml.max_open_jobs", "5");
        attributes.put("non-ml-attribute", "should be filtered out");
        TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9000);

        DiscoveryNode node = new DiscoveryNode("df-node-name", "df-node-id", transportAddress, attributes,
                Set.of(),
                Version.CURRENT);

        Response.DatafeedStats stats = new Response.DatafeedStats("df-id", DatafeedState.STARTED, node, null);

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytes;
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
            bytes = BytesReference.bytes(builder);
        }

        Map<String, Object> dfStatsMap = XContentHelper.convertToMap(bytes, randomBoolean(), xContentType).v2();

        assertThat(dfStatsMap.size(), is(equalTo(3)));
        assertThat(dfStatsMap, hasEntry("datafeed_id", "df-id"));
        assertThat(dfStatsMap, hasEntry("state", "started"));
        assertThat(dfStatsMap, hasKey("node"));

        Map<String, Object> nodeMap = (Map<String, Object>) dfStatsMap.get("node");
        assertThat(nodeMap, hasEntry("id", "df-node-id"));
        assertThat(nodeMap, hasEntry("name", "df-node-name"));
        assertThat(nodeMap, hasKey("ephemeral_id"));
        assertThat(nodeMap, hasKey("transport_address"));
        assertThat(nodeMap, hasKey("attributes"));

        Map<String, Object> nodeAttributes = (Map<String, Object>) nodeMap.get("attributes");
        assertThat(nodeAttributes.size(), is(equalTo(2)));
        assertThat(nodeAttributes, hasEntry("ml.enabled", "true"));
        assertThat(nodeAttributes, hasEntry("ml.max_open_jobs", "5"));
    }
}
