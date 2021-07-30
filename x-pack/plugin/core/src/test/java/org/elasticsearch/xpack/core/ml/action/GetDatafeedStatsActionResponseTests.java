/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStatsTests;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateActionResponseTests.randomRunningState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class GetDatafeedStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<Response.DatafeedStats> datafeedStatsList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String datafeedId = randomAlphaOfLength(10);
            DatafeedState datafeedState = randomFrom(DatafeedState.values());
            DiscoveryNode node =
                randomBoolean()
                    ? null
                    : new DiscoveryNode("_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
            String explanation = randomBoolean() ? null : randomAlphaOfLength(3);
            DatafeedTimingStats timingStats = randomBoolean() ? null : DatafeedTimingStatsTests.createRandom();
            Response.DatafeedStats datafeedStats = new Response.DatafeedStats(
                datafeedId,
                datafeedState,
                node,
                explanation,
                timingStats,
                randomBoolean() ? null : randomRunningState()
            );
            datafeedStatsList.add(datafeedStats);
        }

        result = new Response(new QueryPage<>(datafeedStatsList, datafeedStatsList.size(), DatafeedConfig.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
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

        DatafeedTimingStats timingStats =
            new DatafeedTimingStats("my-job-id", 5, 10, 100.0, new ExponentialAverageCalculationContext(50.0, null, null));

        Response.DatafeedStats stats = new Response.DatafeedStats(
            "df-id",
            DatafeedState.STARTED,
            node,
            null,
            timingStats,
            randomRunningState()
        );

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytes;
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
            bytes = BytesReference.bytes(builder);
        }

        Map<String, Object> dfStatsMap = XContentHelper.convertToMap(bytes, randomBoolean(), xContentType).v2();

        assertThat(dfStatsMap.size(), is(equalTo(5)));
        assertThat(dfStatsMap, hasEntry("datafeed_id", "df-id"));
        assertThat(dfStatsMap, hasEntry("state", "started"));
        assertThat(dfStatsMap, hasKey("node"));
        assertThat(dfStatsMap, hasKey("timing_stats"));
        assertThat(dfStatsMap, hasKey("running_state"));

        Map<String, Object> runningStateMap = (Map<String, Object>) dfStatsMap.get("running_state");
        assertThat(runningStateMap, hasKey("real_time_configured"));
        assertThat(runningStateMap, hasKey("real_time_running"));

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

        Map<String, Object> timingStatsMap = (Map<String, Object>) dfStatsMap.get("timing_stats");
        assertThat(timingStatsMap.size(), is(equalTo(6)));
        assertThat(timingStatsMap, hasEntry("job_id", "my-job-id"));
        assertThat(timingStatsMap, hasEntry("search_count", 5));
        assertThat(timingStatsMap, hasEntry("bucket_count", 10));
        assertThat(timingStatsMap, hasEntry("total_search_time_ms", 100.0));
        assertThat(timingStatsMap, hasEntry("average_search_time_per_bucket_ms", 10.0));
        assertThat(timingStatsMap, hasEntry("exponential_average_search_time_per_hour_ms", 50.0));
    }
}
