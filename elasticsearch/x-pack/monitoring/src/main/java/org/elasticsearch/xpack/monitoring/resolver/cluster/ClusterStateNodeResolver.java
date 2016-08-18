/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStateNodeMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class ClusterStateNodeResolver extends MonitoringIndexNameResolver.Timestamped<ClusterStateNodeMonitoringDoc> {

    public static final String TYPE = "node";

    public ClusterStateNodeResolver(MonitoredSystem id, Settings settings) {
        super(id, settings);
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
        static final String STATE_UUID = "state_uuid";
        static final String NODE = "node";
        static final String ID = "id";
    }
}
