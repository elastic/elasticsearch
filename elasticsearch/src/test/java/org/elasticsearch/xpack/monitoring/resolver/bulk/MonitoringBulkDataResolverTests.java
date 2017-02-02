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
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests {@link MonitoringBulkDataResolver}.
 */
public class MonitoringBulkDataResolverTests extends MonitoringIndexNameResolverTestCase<MonitoringBulkDoc, MonitoringBulkDataResolver> {

    private final String id = randomBoolean() ? randomAsciiOfLength(35) : null;

    @Override
    protected MonitoringBulkDoc newMonitoringDoc() {
        MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), MonitoringTemplateUtils.TEMPLATE_VERSION,
                                                      MonitoringIndex.DATA, "kibana", id,
                                                      new BytesArray("{\"field1\" : \"value1\"}"), XContentType.JSON);

        if (randomBoolean()) {
            doc.setClusterUUID(randomAsciiOfLength(5));
        }

        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        return doc;
    }

    @Override
    public void testId() {
        MonitoringBulkDoc doc = newMonitoringDoc();
        
        assertThat(newResolver(doc).id(doc), sameInstance(id));
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testMonitoringBulkResolver() throws Exception {
        MonitoringBulkDoc doc = newMonitoringDoc();

        MonitoringBulkDataResolver resolver = newResolver(doc);
        assertThat(resolver.index(doc), equalTo(".monitoring-data-2"));
        assertThat(resolver.type(doc), equalTo(doc.getType()));
        assertThat(resolver.id(doc), sameInstance(id));

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "kibana",
                        "kibana.field1"), XContentType.JSON);
    }
}
