/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class DataResolverTests extends MonitoringIndexNameResolverTestCase {

    @Override
    protected MonitoringIndexNameResolver<MonitoringDoc> newResolver() {
        return newDataResolver();
    }

    @Override
    protected MonitoringDoc newMonitoringDoc() {
        MonitoringDoc doc = new MonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
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
        assertThat(newDataResolver().index(newMonitoringDoc()), equalTo(".monitoring-data-" + MonitoringTemplateUtils.TEMPLATE_VERSION));
    }

    private MonitoringIndexNameResolver.Data<MonitoringDoc> newDataResolver() {
        return new MonitoringIndexNameResolver.Data<MonitoringDoc>() {
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
