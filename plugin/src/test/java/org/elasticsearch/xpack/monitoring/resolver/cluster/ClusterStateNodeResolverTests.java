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
        ClusterStateNodeMonitoringDoc doc = new ClusterStateNodeMonitoringDoc(randomMonitoringId(),
                randomAlphaOfLength(2), randomAlphaOfLength(5), 1437580442979L,
                new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                UUID.randomUUID().toString(), randomAlphaOfLength(5));
        return doc;
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testClusterStateNodeResolver() throws Exception {
        ClusterStateNodeMonitoringDoc doc = newMonitoringDoc();

        ClusterStateNodeResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "state_uuid",
                        "node.id"), XContentType.JSON);
    }
}
