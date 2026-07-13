/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.stats;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class GetSecurityStatsNodeResponse extends BaseNodeResponse implements ToXContentObject {

    private static final TransportVersion ROLES_SECURITY_STATS = TransportVersion.fromName("roles_security_stats");

    @Nullable
    private final Map<String, Object> rolesStoreStats;

    public GetSecurityStatsNodeResponse(final StreamInput in) throws IOException {
        super(in);
        this.rolesStoreStats = in.getTransportVersion().supports(ROLES_SECURITY_STATS) ? in.readGenericMap() : null;
    }

    public GetSecurityStatsNodeResponse(final DiscoveryNode node, @Nullable final Map<String, Object> rolesStoreStats) {
        super(node);
        this.rolesStoreStats = rolesStoreStats;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(ROLES_SECURITY_STATS)) {
            out.writeGenericMap(rolesStoreStats);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        if (rolesStoreStats != null) {
            builder.field("roles", rolesStoreStats);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof GetSecurityStatsNodeResponse that
                && Objects.equals(getNode(), that.getNode())
                && Objects.equals(rolesStoreStats, that.rolesStoreStats));
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNode(), rolesStoreStats);
    }

    // for testing
    @Nullable
    Map<String, Object> getRolesStoreStats() {
        return rolesStoreStats == null ? null : Collections.unmodifiableMap(rolesStoreStats);
    }

    // for testing
    DiscoveryNode getDiscoveryNode() {
        return getNode();
    }
}
