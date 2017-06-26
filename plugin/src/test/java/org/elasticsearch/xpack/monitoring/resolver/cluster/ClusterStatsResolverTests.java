/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.monitoring.MonitoringFeatureSet;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import java.util.Collections;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStatsResolverTests extends MonitoringIndexNameResolverTestCase<ClusterStatsMonitoringDoc, ClusterStatsResolver> {

    @Override
    protected ClusterStatsMonitoringDoc newMonitoringDoc() {
        try {
            License.Builder licenseBuilder = License.builder()
                    .uid(UUID.randomUUID().toString())
                    .type("trial")
                    .issuer("elasticsearch")
                    .issuedTo("customer")
                    .maxNodes(5)
                    .issueDate(1437580442979L)
                    .expiryDate(1437580442979L + TimeValue.timeValueHours(2).getMillis());

            final String nodeUuid = "the-master-nodes-uuid";
            final ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);
            final DiscoveryNodes nodes = mock(DiscoveryNodes.class);
            final DiscoveryNode node =
                    new DiscoveryNode(nodeUuid, new TransportAddress(TransportAddress.META_ADDRESS, 9300), Version.CURRENT);

            when(nodes.getMasterNodeId()).thenReturn(nodeUuid);
            when(nodes.iterator()).thenReturn(Collections.singleton(node).iterator());

            return new ClusterStatsMonitoringDoc(randomMonitoringId(),
                    randomAlphaOfLength(2), randomAlphaOfLength(5),
                    Math.abs(randomLong()),
                    new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                    randomAlphaOfLength(5),
                    randomFrom(Version.V_5_0_0, Version.CURRENT).toString(),
                    licenseBuilder.build(),
                    Collections.singletonList(new MonitoringFeatureSet.Usage(randomBoolean(), randomBoolean(), emptyMap())),
                    new ClusterStatsResponse(
                            Math.abs(randomLong()),
                            clusterName,
                            Collections.emptyList(),
                            Collections.emptyList()),
                    new ClusterState(clusterName, randomLong(), "a-real-state-uuid", null, null, nodes, null, null, false),
                    randomFrom(ClusterHealthStatus.values())
                );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generated random ClusterStatsMonitoringDoc", e);
        }
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testClusterInfoResolver() throws Exception {
        ClusterStatsMonitoringDoc doc = newMonitoringDoc();

        ClusterStatsResolver resolver = newResolver();
        assertThat(resolver.index(doc), startsWith(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION));
        assertSource(resolver.source(doc, XContentType.JSON),
                     Sets.newHashSet(
                         "cluster_uuid",
                         "timestamp",
                         "type",
                         "source_node",
                         "cluster_name",
                         "version",
                         "license",
                         "cluster_stats",
                         "cluster_state.status",
                         "cluster_state.version",
                         "cluster_state.state_uuid",
                         "cluster_state.master_node",
                         "cluster_state.nodes",
                         "stack_stats.xpack"), XContentType.JSON);
    }
}
