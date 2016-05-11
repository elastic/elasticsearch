/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.action.MonitoringBulkDoc;
import org.elasticsearch.marvel.action.MonitoringIndex;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;

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
    protected MonitoringBulkDoc newMarvelDoc() {
        MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), Version.CURRENT.toString(),
                                                      MonitoringIndex.DATA, "kibana", id,
                                                      new BytesArray("{\"field1\" : \"value1\"}"));

        if (randomBoolean()) {
            doc.setClusterUUID(randomAsciiOfLength(5));
        }

        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
        return doc;
    }

    @Override
    public void testId() {
        MonitoringBulkDoc doc = newMarvelDoc();
        
        assertThat(newResolver(doc).id(doc), sameInstance(id));
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testMonitoringBulkResolver() throws Exception {
        MonitoringBulkDoc doc = newMarvelDoc();

        MonitoringBulkDataResolver resolver = newResolver(doc);
        assertThat(resolver.index(doc), equalTo(".monitoring-data-2"));
        assertThat(resolver.type(doc), equalTo(doc.getType()));
        assertThat(resolver.id(doc), sameInstance(id));

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "kibana",
                "kibana.field1");
    }
}
