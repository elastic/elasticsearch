/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStateNodeMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateNodeResolverTests extends
        MonitoringIndexNameResolverTestCase<ClusterStateNodeMonitoringDoc, ClusterStateNodeResolver> {

    @Override
    protected ClusterStateNodeMonitoringDoc newMonitoringDoc() {
        ClusterStateNodeMonitoringDoc doc = new ClusterStateNodeMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        doc.setNodeId(UUID.randomUUID().toString());
        doc.setStateUUID(UUID.randomUUID().toString());
        return doc;
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    @Override
    protected boolean checkResolvedId() {
        return false;
    }

    public void testClusterStateNodeResolver() throws Exception {
        final String nodeId = UUID.randomUUID().toString();
        final String stateUUID = UUID.randomUUID().toString();

        ClusterStateNodeMonitoringDoc doc = newMonitoringDoc();
        doc.setNodeId(nodeId);
        doc.setStateUUID(stateUUID);
        doc.setTimestamp(1437580442979L);

        ClusterStateNodeResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(ClusterStateNodeResolver.TYPE));
        assertThat(resolver.id(doc), nullValue());

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "state_uuid",
                        "node.id"), XContentType.JSON);
    }
}
