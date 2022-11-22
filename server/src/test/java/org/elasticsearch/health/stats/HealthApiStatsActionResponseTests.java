/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.stats;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class HealthApiStatsActionResponseTests extends ESTestCase {

    public void testMerging() {

        HealthApiStatsAction.Response.Node nodeResponse1 = new HealthApiStatsAction.Response.Node(
            new DiscoveryNode("remote_node", buildNewFakeTransportAddress(), Version.CURRENT)
        );
        {
            Counters counters = new Counters();
            counters.inc("merged.metric", randomIntBetween(1, 10));
            counters.inc("only.one.metric", randomIntBetween(1, 10));
            nodeResponse1.setStats(counters);
            nodeResponse1.setStatuses(Set.of(HealthStatus.RED, HealthStatus.GREEN));
            nodeResponse1.setIndicators(Map.of(HealthStatus.RED, Set.of("one_red_indicator", "shared_red_indicator")));
            nodeResponse1.setDiagnoses(
                Map.of(HealthStatus.RED, Set.of("one:red:diagnosis"), HealthStatus.YELLOW, Set.of("one:yellow:diagnosis"))
            );
        }
        HealthApiStatsAction.Response.Node nodeResponse2 = new HealthApiStatsAction.Response.Node(
            new DiscoveryNode("remote_node", buildNewFakeTransportAddress(), Version.CURRENT)
        );

        HealthApiStatsAction.Response.Node nodeResponse3 = new HealthApiStatsAction.Response.Node(
            new DiscoveryNode("remote_node", buildNewFakeTransportAddress(), Version.CURRENT)
        );
        {
            Counters counters = new Counters();
            counters.inc("merged.metric", randomIntBetween(1, 10));
            counters.inc("only.third.metric", randomIntBetween(1, 10));
            nodeResponse3.setStats(counters);
            nodeResponse3.setStatuses(Set.of(HealthStatus.YELLOW, HealthStatus.GREEN));
            nodeResponse3.setIndicators(
                Map.of(HealthStatus.RED, Set.of("shared_red_indicator"), HealthStatus.YELLOW, Set.of("one_yellow_indicator"))
            );
            nodeResponse3.setDiagnoses(
                Map.of(HealthStatus.RED, Set.of("another:red:diagnosis"), HealthStatus.YELLOW, Set.of("another:yellow:diagnosis"))
            );
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
        assertThat(response.getStatuses(), equalTo(Set.of(HealthStatus.RED, HealthStatus.YELLOW, HealthStatus.GREEN)));
        assertThat(
            response.getIndicators(),
            equalTo(
                Map.of(
                    HealthStatus.RED,
                    Set.of("shared_red_indicator", "one_red_indicator"),
                    HealthStatus.YELLOW,
                    Set.of("one_yellow_indicator")
                )
            )
        );
        assertThat(
            response.getDiagnoses(),
            equalTo(
                Map.of(
                    HealthStatus.RED,
                    Set.of("one:red:diagnosis", "another:red:diagnosis"),
                    HealthStatus.YELLOW,
                    Set.of("one:yellow:diagnosis", "another:yellow:diagnosis")
                )
            )
        );
    }
}
