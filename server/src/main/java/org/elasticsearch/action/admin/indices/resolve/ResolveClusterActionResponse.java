/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ResolveClusterActionResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField CONNECTED_FIELD = new ParseField("connected");
    private static final ParseField SKIP_UNAVAILABLE_FIELD = new ParseField("skip_unavailable");
    private static final ParseField MATCHING_INDICES_FIELD = new ParseField("matching_indices");
    private static final ParseField ES_VERSION_FIELD = new ParseField("version");
    private static final ParseField ERROR_FIELD = new ParseField("error");

    private final Map<String, ResolveClusterInfo> infoMap;

    public ResolveClusterActionResponse(Map<String, ResolveClusterInfo> infoMap) {
        this.infoMap = infoMap;
    }

    public ResolveClusterActionResponse(StreamInput in) throws IOException {
        this.infoMap = in.readImmutableMap(ResolveClusterInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least version "
                    + TransportVersions.V_8_13_0.toReleaseVersion()
                    + " but was "
                    + out.getTransportVersion().toReleaseVersion()
            );
        }
        out.writeMap(infoMap, StreamOutput::writeWriteable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, ResolveClusterInfo> entry : infoMap.entrySet()) {
            String clusterAlias = entry.getKey();
            if (clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                clusterAlias = SearchResponse.LOCAL_CLUSTER_NAME_REPRESENTATION;
            }
            builder.startObject(clusterAlias);
            ResolveClusterInfo clusterInfo = entry.getValue();
            builder.field(CONNECTED_FIELD.getPreferredName(), clusterInfo.isConnected());
            builder.field(SKIP_UNAVAILABLE_FIELD.getPreferredName(), clusterInfo.getSkipUnavailable());
            if (clusterInfo.getError() != null) {
                builder.field(ERROR_FIELD.getPreferredName(), clusterInfo.getError());
            }
            if (clusterInfo.getMatchingIndices() != null) {
                builder.field(MATCHING_INDICES_FIELD.getPreferredName(), clusterInfo.getMatchingIndices());
            }
            Build build = clusterInfo.getBuild();
            if (build != null) {
                builder.startObject(ES_VERSION_FIELD.getPreferredName())
                    .field("number", build.qualifiedVersion())
                    .field("build_flavor", build.flavor())  // is "stateless" for stateless projects
                    .field("minimum_wire_compatibility_version", build.minWireCompatVersion())
                    .field("minimum_index_compatibility_version", build.minIndexCompatVersion())
                    .endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResolveClusterActionResponse response = (ResolveClusterActionResponse) o;
        return infoMap.equals(response.infoMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(infoMap);
    }

    public Map<String, ResolveClusterInfo> getResolveClusterInfo() {
        return infoMap;
    }
}
