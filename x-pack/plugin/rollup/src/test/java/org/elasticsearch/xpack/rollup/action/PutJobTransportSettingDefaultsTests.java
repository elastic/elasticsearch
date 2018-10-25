/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isIn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PutJobTransportSettingDefaultsTests extends ESTestCase {

    public void testSettingDefaultValues() {
        GroupConfig group = ConfigTestHelpers.randomGroupConfig(random());
        final List<MetricConfig> metrics = ConfigTestHelpers.randomMetricsConfigs(random());
        RollupJobConfig rollupJobConfig = new RollupJobConfig(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            "rollup_" + randomAlphaOfLength(10),
            ConfigTestHelpers.randomCron(),
            randomIntBetween(1, 10),
            group,
            metrics,
            null);


        PutRollupJobAction.Request request = new PutRollupJobAction.Request(rollupJobConfig);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(Version.V_6_0_0);
        when(clusterState.nodes()).thenReturn(nodes);

        RollupJobConfig newRollupJobConfig = TransportPutRollupJobAction.addDefaultMetricsIfNecessary(request.getConfig(), clusterState);

        assertThat(metrics, containsInAnyOrder(newRollupJobConfig.getMetricsConfig().toArray(new MetricConfig[0])));
    }

    public void testSettingDefaultValuesWithProperMinVersion() {
        DateHistogramGroupConfig dateHistogram = ConfigTestHelpers.randomDateHistogramGroupConfig(random());
        HistogramGroupConfig histogram = ConfigTestHelpers.randomHistogramGroupConfig(random());
        TermsGroupConfig terms = random().nextBoolean() ? ConfigTestHelpers.randomTermsGroupConfig(random()) : null;
        GroupConfig group = new GroupConfig(dateHistogram, histogram, terms);
        final List<MetricConfig> metrics = ConfigTestHelpers.randomMetricsConfigs(random());
        RollupJobConfig rollupJobConfig = new RollupJobConfig(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            "rollup_" + randomAlphaOfLength(10),
            ConfigTestHelpers.randomCron(),
            randomIntBetween(1, 10),
            group,
            metrics,
            null);


        PutRollupJobAction.Request request = new PutRollupJobAction.Request(rollupJobConfig);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(Version.V_6_5_0);
        when(clusterState.nodes()).thenReturn(nodes);
        List<String> histoFields = Arrays.asList(group.getHistogram().getFields());

        RollupJobConfig newRollupJobConfig = TransportPutRollupJobAction.addDefaultMetricsIfNecessary(request.getConfig(), clusterState);

        Set<String> metricFields = newRollupJobConfig.getMetricsConfig().stream().map(MetricConfig::getField).collect(Collectors.toSet());
        assertThat(group.getDateHistogram().getField(), isIn(metricFields));
        newRollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
            if (metricConfig.getField().equals(group.getDateHistogram().getField())) {
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("max", "min"));
            } else if (histoFields.contains(metricConfig.getField())) {
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("max", "min"));
            }
        });
    }

    public void testSettingDefaultValuesWithProperMinVersionAndUserProvideMetrics() {
        DateHistogramGroupConfig dateHistogram = ConfigTestHelpers.randomDateHistogramGroupConfig(random());
        HistogramGroupConfig histogram = ConfigTestHelpers.randomHistogramGroupConfig(random());
        TermsGroupConfig terms = random().nextBoolean() ? ConfigTestHelpers.randomTermsGroupConfig(random()) : null;
        GroupConfig group = new GroupConfig(dateHistogram, histogram, terms);
        final List<MetricConfig> metrics = new ArrayList<>(2 + group.getHistogram().getFields().length);
        metrics.add(new MetricConfig("anyfield", Arrays.asList("max", "min", "avg")));
        metrics.add(new MetricConfig(group.getDateHistogram().getField(), Collections.singletonList("min")));
        for (String histogramField : group.getHistogram().getFields()) {
            metrics.add(new MetricConfig(histogramField, Collections.singletonList("min")));
        }
        RollupJobConfig rollupJobConfig = new RollupJobConfig(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            "rollup_" + randomAlphaOfLength(10),
            ConfigTestHelpers.randomCron(),
            randomIntBetween(1, 10),
            group,
            metrics,
            null);


        PutRollupJobAction.Request request = new PutRollupJobAction.Request(rollupJobConfig);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(Version.V_6_5_0);
        when(clusterState.nodes()).thenReturn(nodes);

        RollupJobConfig newRollupJobConfig = TransportPutRollupJobAction.addDefaultMetricsIfNecessary(request.getConfig(), clusterState);

        List<String> histoFields = Arrays.asList(group.getHistogram().getFields());
        newRollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
            if (metricConfig.getField().equals(group.getDateHistogram().getField())) {
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("min"));
            } else if (histoFields.contains(metricConfig.getField())) {
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("min"));
            }
        });
    }
}
