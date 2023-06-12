/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;
import org.elasticsearch.xpack.core.common.stats.EnumCounters;

import static org.hamcrest.Matchers.equalTo;

public class AnalyticsStatsActionNodeResponseTests extends AbstractWireSerializingTestCase<AnalyticsStatsAction.NodeResponse> {

    @Override
    protected Writeable.Reader<AnalyticsStatsAction.NodeResponse> instanceReader() {
        return AnalyticsStatsAction.NodeResponse::new;
    }

    @Override
    protected AnalyticsStatsAction.NodeResponse createTestInstance() {
        String nodeName = randomAlphaOfLength(10);
        DiscoveryNode node = DiscoveryNodeUtils.create(nodeName);
        EnumCounters<AnalyticsStatsAction.Item> counters = new EnumCounters<>(AnalyticsStatsAction.Item.class);
        for (AnalyticsStatsAction.Item item : AnalyticsStatsAction.Item.values()) {
            if (randomBoolean()) {
                counters.inc(item, randomLongBetween(0, 1000));
            }
        }
        return new AnalyticsStatsAction.NodeResponse(node, counters);
    }

    @Override
    protected AnalyticsStatsAction.NodeResponse mutateInstance(AnalyticsStatsAction.NodeResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testItemEnum() {
        int i = 0;
        // We rely on the ordinals for serialization, so they shouldn't change between version
        assertThat(AnalyticsStatsAction.Item.BOXPLOT.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.CUMULATIVE_CARDINALITY.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.STRING_STATS.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.TOP_METRICS.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.T_TEST.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.MOVING_PERCENTILES.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.NORMALIZE.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.RATE.ordinal(), equalTo(i++));
        assertThat(AnalyticsStatsAction.Item.MULTI_TERMS.ordinal(), equalTo(i++));
        // Please add tests for newly added items here
        assertThat(AnalyticsStatsAction.Item.values().length, equalTo(i));
    }
}
