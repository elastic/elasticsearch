/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.cluster;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.collector.cluster.DiscoveryNodeMonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class DiscoveryNodeResolver extends MonitoringIndexNameResolver.Data<DiscoveryNodeMonitoringDoc> {

    public static final String TYPE = "node";

    public DiscoveryNodeResolver(int version) {
        super(version);
    }

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
            for (ObjectObjectCursor<String, String> attr : node.getAttributes()) {
                builder.field(attr.key, attr.value);
            }
            builder.endObject();
            builder.field(Fields.ID, node.getId());
        }

        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString NODE = new XContentBuilderString(TYPE);
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString TRANSPORT_ADDRESS = new XContentBuilderString("transport_address");
        static final XContentBuilderString ATTRIBUTES = new XContentBuilderString("attributes");
        static final XContentBuilderString ID = new XContentBuilderString("id");
    }
}
