/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.node;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.collector.cluster.DiscoveryNodeMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;
import org.elasticsearch.marvel.agent.resolver.cluster.DiscoveryNodeResolver;
import org.elasticsearch.test.VersionUtils;

import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class DiscoveryNodeResolverTests extends MonitoringIndexNameResolverTestCase<DiscoveryNodeMonitoringDoc, DiscoveryNodeResolver> {

    @Override
    protected DiscoveryNodeMonitoringDoc newMarvelDoc() {
        DiscoveryNodeMonitoringDoc doc = new DiscoveryNodeMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
        doc.setNode(new DiscoveryNode(randomAsciiOfLength(3), UUID.randomUUID().toString(),
                DummyTransportAddress.INSTANCE, emptyMap(), emptySet(),
                VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), Version.CURRENT)));
        return doc;
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testDiscoveryNodeResolver() throws Exception {
        DiscoveryNodeMonitoringDoc doc = newMarvelDoc();
        doc.setTimestamp(1437580442979L);

        DiscoveryNodeResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-data-" + MarvelTemplateUtils.TEMPLATE_VERSION));
        assertThat(resolver.type(doc), equalTo(DiscoveryNodeResolver.TYPE));
        assertThat(resolver.id(doc), equalTo(doc.getNode().getId()));

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "node.id",
                "node.name",
                "node.transport_address",
                "node.attributes");
    }
}
