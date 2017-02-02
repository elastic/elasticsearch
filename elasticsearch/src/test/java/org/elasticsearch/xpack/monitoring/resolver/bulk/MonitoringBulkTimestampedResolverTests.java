/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.monitoring.action.MonitoringIndex;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests {@link MonitoringBulkTimestampedResolver}.
 */
public class MonitoringBulkTimestampedResolverTests
        extends MonitoringIndexNameResolverTestCase<MonitoringBulkDoc, MonitoringBulkTimestampedResolver> {

    @Override
    protected MonitoringBulkDoc newMonitoringDoc() {
        MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), MonitoringTemplateUtils.TEMPLATE_VERSION,
                                                      MonitoringIndex.TIMESTAMPED, "kibana_stats", null,
                                                      new BytesArray("{\"field1\" : \"value1\"}"), XContentType.JSON);

        doc.setTimestamp(1437580442979L);
        if (randomBoolean()) {
            doc.setId(randomAsciiOfLength(35));
        }
        if (randomBoolean()) {
            doc.setClusterUUID(randomAsciiOfLength(5));
        }

        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setSourceNode(new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        return doc;
    }

    @Override
    protected boolean checkResolvedId() {
        return false;
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testMonitoringBulkResolver() throws Exception {
        MonitoringBulkDoc doc = newMonitoringDoc();

        MonitoringBulkTimestampedResolver resolver = newResolver(doc);
        assertThat(resolver.index(doc), equalTo(".monitoring-kibana-2-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(doc.getType()));
        assertThat(resolver.id(doc), nullValue());

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "kibana_stats",
                        "kibana_stats.field1"), XContentType.JSON);
    }
}
