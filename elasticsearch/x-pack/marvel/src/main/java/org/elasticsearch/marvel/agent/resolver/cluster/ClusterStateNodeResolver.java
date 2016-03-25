/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.cluster;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateNodeMonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class ClusterStateNodeResolver extends MonitoringIndexNameResolver.Timestamped<ClusterStateNodeMonitoringDoc> {

    public static final String TYPE = "node";

    public ClusterStateNodeResolver(MonitoredSystem id, int version, Settings settings) {
        super(id, version, settings);
    }

    @Override
    public String type(ClusterStateNodeMonitoringDoc document) {
        return TYPE;
    }

    @Override
    protected void buildXContent(ClusterStateNodeMonitoringDoc document,
                                 XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(Fields.STATE_UUID, document.getStateUUID());
        builder.startObject(Fields.NODE);
        builder.field(Fields.ID, document.getNodeId());
        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString STATE_UUID = new XContentBuilderString("state_uuid");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString ID = new XContentBuilderString("id");
    }
}
