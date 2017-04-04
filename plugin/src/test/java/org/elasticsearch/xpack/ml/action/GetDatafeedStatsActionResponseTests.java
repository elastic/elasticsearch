/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction.Response;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

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

}
