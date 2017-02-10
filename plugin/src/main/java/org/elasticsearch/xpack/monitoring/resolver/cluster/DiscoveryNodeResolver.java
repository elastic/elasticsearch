/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.collector.cluster.DiscoveryNodeMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;
import java.util.Map;

public class DiscoveryNodeResolver extends MonitoringIndexNameResolver.Data<DiscoveryNodeMonitoringDoc> {

    public static final String TYPE = "node";

    @Override
    public String type(DiscoveryNodeMonitoringDoc document) {
        return TYPE;
    }

    @Override
    public String id(DiscoveryNodeMonitoringDoc document) {
        return document.getNode().getId();
    }

    @Override
    protected void buildXContent(DiscoveryNodeMonitoringDoc document,
                                 XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.NODE);

        DiscoveryNode node = document.getNode();
        if (node != null) {
            builder.field(Fields.NAME, node.getName());
            builder.field(Fields.TRANSPORT_ADDRESS, node.getAddress().toString());

            builder.startObject(Fields.ATTRIBUTES);
            for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            builder.field(Fields.ID, node.getId());
        }

        builder.endObject();
    }

    static final class Fields {
        static final String NODE = TYPE;
        static final String NAME = "name";
        static final String TRANSPORT_ADDRESS = "transport_address";
        static final String ATTRIBUTES = "attributes";
        static final String ID = "id";
    }
}
