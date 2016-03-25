/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodeResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterInfoMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;

import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class ClusterInfoResolverTests extends MonitoringIndexNameResolverTestCase<ClusterInfoMonitoringDoc, ClusterInfoResolver> {

    @Override
    protected ClusterInfoMonitoringDoc newMarvelDoc() {
        try {
            License.Builder licenseBuilder = License.builder()
                    .uid(UUID.randomUUID().toString())
                    .type("trial")
                    .issuer("elasticsearch")
                    .issuedTo("customer")
                    .maxNodes(5)
                    .issueDate(1437580442979L)
                    .expiryDate(1437580442979L + TimeValue.timeValueHours(2).getMillis());

            ClusterInfoMonitoringDoc doc = new ClusterInfoMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
            doc.setClusterUUID(randomAsciiOfLength(5));
            doc.setTimestamp(Math.abs(randomLong()));
            doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
            doc.setVersion(randomFrom(Version.V_2_0_0, Version.CURRENT).toString());
            doc.setLicense(licenseBuilder.build());
            doc.setClusterName(randomAsciiOfLength(5));
            doc.setClusterStats(new ClusterStatsResponse(Math.abs(randomLong()), ClusterName.DEFAULT,
                    randomAsciiOfLength(5), new ClusterStatsNodeResponse[]{}));
            return doc;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generated random ClusterInfoMarvelDoc", e);
        }
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testClusterInfoResolver() throws Exception {
        String clusterUUID = UUID.randomUUID().toString();

        ClusterInfoMonitoringDoc doc = newMarvelDoc();
        doc.setClusterUUID(clusterUUID);

        ClusterInfoResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-data-" + MarvelTemplateUtils.TEMPLATE_VERSION));
        assertThat(resolver.type(doc), equalTo(ClusterInfoResolver.TYPE));
        assertThat(resolver.id(doc), equalTo(clusterUUID));

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "cluster_name",
                "version",
                "license",
                "cluster_stats");
    }
}
