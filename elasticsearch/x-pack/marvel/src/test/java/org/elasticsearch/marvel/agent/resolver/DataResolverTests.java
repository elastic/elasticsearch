/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DataResolverTests extends MonitoringIndexNameResolverTestCase {

    private int randomVersion = randomIntBetween(0, 100);

    @Override
    protected MonitoringIndexNameResolver<MonitoringDoc> newResolver() {
        return newDataResolver(randomVersion);
    }

    @Override
    protected MonitoringDoc newMarvelDoc() {
        MonitoringDoc doc = new MonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, Version.CURRENT));
        return doc;
    }

    @Override
    protected boolean checkResolvedType() {
        return false;
    }

    @Override
    protected boolean checkResolvedId() {
        return false;
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testDataResolver() {
        assertThat(newDataResolver(randomVersion).index(newMarvelDoc()), equalTo(".monitoring-data-" + randomVersion));
        assertThat(newDataResolver(42).index(newMarvelDoc()), equalTo(".monitoring-data-42"));
    }

    private MonitoringIndexNameResolver.Data<MonitoringDoc> newDataResolver(int randomVersion) {
        return new MonitoringIndexNameResolver.Data<MonitoringDoc>(randomVersion) {
            @Override
            public String type(MonitoringDoc document) {
                return null;
            }

            @Override
            public String id(MonitoringDoc document) {
                return null;
            }

            @Override
            protected void buildXContent(MonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
            }
        };
    }
}
