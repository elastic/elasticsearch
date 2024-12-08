/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;

public final class SearchContextIdForNode implements Writeable {
    private final String node;
    private final ShardSearchContextId searchContextId;
    private final String clusterAlias;

    /**
     * Contains the details required to retrieve a {@link ShardSearchContextId} for a shard on a specific node.
     *
     * @param clusterAlias The alias of the cluster, or {@code null} if the shard is local.
     * @param node The target node where the search context ID is defined, or {@code null} if the shard is missing or unavailable.
     * @param searchContextId The {@link ShardSearchContextId}, or {@code null} if the shard is missing or unavailable.
     */
    SearchContextIdForNode(@Nullable String clusterAlias, @Nullable String node, @Nullable ShardSearchContextId searchContextId) {
        this.node = node;
        this.clusterAlias = clusterAlias;
        this.searchContextId = searchContextId;
    }

    SearchContextIdForNode(StreamInput in) throws IOException {
        boolean allowNull = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0);
        this.node = allowNull ? in.readOptionalString() : in.readString();
        this.clusterAlias = in.readOptionalString();
        this.searchContextId = allowNull ? in.readOptionalWriteable(ShardSearchContextId::new) : new ShardSearchContextId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean allowNull = out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0);
        if (allowNull) {
            out.writeOptionalString(node);
        } else {
            if (node == null) {
                // We should never set a null node if the cluster is not fully upgraded to a version that can handle it.
                throw new IOException(
                    "Cannot write null node value to a node in version "
                        + out.getTransportVersion().toReleaseVersion()
                        + ". The target node must be specified to retrieve the ShardSearchContextId."
                );
            }
            out.writeString(node);
        }
        out.writeOptionalString(clusterAlias);
        if (allowNull) {
            out.writeOptionalWriteable(searchContextId);
        } else {
            if (searchContextId == null) {
                // We should never set a null search context id if the cluster is not fully upgraded to a version that can handle it.
                throw new IOException(
                    "Cannot write null search context ID to a node in version "
                        + out.getTransportVersion().toReleaseVersion()
                        + ". A valid search context ID is required to identify the shard's search context in this version."
                );
            }
            searchContextId.writeTo(out);
        }
    }

    @Nullable
    public String getNode() {
        return node;
    }

    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    @Nullable
    public ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    @Override
    public String toString() {
        return "SearchContextIdForNode{"
            + "node='"
            + node
            + '\''
            + ", seachContextId="
            + searchContextId
            + ", clusterAlias='"
            + clusterAlias
            + '\''
            + '}';
    }
}
