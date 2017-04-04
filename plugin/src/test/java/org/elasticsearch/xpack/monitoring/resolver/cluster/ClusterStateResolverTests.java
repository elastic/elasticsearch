/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStateMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateResolverTests extends MonitoringIndexNameResolverTestCase<ClusterStateMonitoringDoc, ClusterStateResolver> {

    @Override
    protected ClusterStateMonitoringDoc newMonitoringDoc() {
        DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(),
                emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(masterNode).add(otherNode).masterNodeId(masterNode.getId()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();

        ClusterStateMonitoringDoc doc = new ClusterStateMonitoringDoc(randomMonitoringId(),
                randomAlphaOfLength(2), randomAlphaOfLength(5), 1437580442979L,
                new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                clusterState, randomFrom(ClusterHealthStatus.values()));

        return doc;
    }

    public void testClusterStateResolver() throws IOException {
        ClusterStateMonitoringDoc doc = newMonitoringDoc();

        ClusterStateResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "cluster_state"), XContentType.JSON);
    }
}
