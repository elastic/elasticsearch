/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateNodeMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;

import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateNodeResolverTests extends
        MonitoringIndexNameResolverTestCase<ClusterStateNodeMonitoringDoc, ClusterStateNodeResolver> {

    @Override
    protected ClusterStateNodeMonitoringDoc newMarvelDoc() {
        ClusterStateNodeMonitoringDoc doc = new ClusterStateNodeMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
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

        ClusterStateNodeMonitoringDoc doc = newMarvelDoc();
        doc.setNodeId(nodeId);
        doc.setStateUUID(stateUUID);
        doc.setTimestamp(1437580442979L);

        ClusterStateNodeResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(ClusterStateNodeResolver.TYPE));
        assertThat(resolver.id(doc), nullValue());

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "state_uuid",
                "node.id");
    }
}
