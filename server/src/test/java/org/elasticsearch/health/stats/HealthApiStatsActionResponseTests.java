/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.stats;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class HealthApiStatsActionResponseTests extends ESTestCase {

    public void testMerging() {

        HealthApiStatsAction.Response.Node nodeResponse1 = new HealthApiStatsAction.Response.Node(DiscoveryNodeUtils.create("remote_node"));
        {
            Counters counters = new Counters();
            counters.inc("merged.metric", randomIntBetween(1, 10));
            counters.inc("only.one.metric", randomIntBetween(1, 10));
            nodeResponse1.setStats(counters);
        }
        HealthApiStatsAction.Response.Node nodeResponse2 = new HealthApiStatsAction.Response.Node(DiscoveryNodeUtils.create("remote_node"));

        HealthApiStatsAction.Response.Node nodeResponse3 = new HealthApiStatsAction.Response.Node(DiscoveryNodeUtils.create("remote_node"));
        {
            Counters counters = new Counters();
            counters.inc("merged.metric", randomIntBetween(1, 10));
            counters.inc("only.third.metric", randomIntBetween(1, 10));
            nodeResponse3.setStats(counters);
        }
        HealthApiStatsAction.Response response = new HealthApiStatsAction.Response(
            ClusterName.DEFAULT,
            List.of(nodeResponse1, nodeResponse2, nodeResponse3),
            List.of()
        );
        Counters stats = response.getStats();
        assertThat(
            stats.get("merged.metric"),
            equalTo(nodeResponse1.getStats().get("merged.metric") + nodeResponse3.getStats().get("merged.metric"))
        );
        assertThat(stats.get("only.one.metric"), equalTo(nodeResponse1.getStats().get("only.one.metric")));
        assertThat(stats.get("only.third.metric"), equalTo(nodeResponse3.getStats().get("only.third.metric")));
    }
}
