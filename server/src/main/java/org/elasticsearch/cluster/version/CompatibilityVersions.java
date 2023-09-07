/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

/**
 * Wraps component version numbers for cluster state
 *
 * <p>Cluster state will need to carry version information for different independently versioned components.
 * This wrapper lets us wrap these versions one level below {@link org.elasticsearch.cluster.ClusterState}.
 * It's similar to {@link org.elasticsearch.cluster.node.VersionInformation}, but this class is meant to
 * be constructed during node startup and hold values from plugins as well.
 *
 * @param transportVersion A transport version, usually a minimum compatible one for a node.
 */
public record CompatibilityVersions(TransportVersion transportVersion) implements Writeable, ToXContentFragment {

    /**
     * Constructs a VersionWrapper collecting all the minimum versions from the values of the map.
     *
     * @param compatibilityVersions A map of strings (typically node identifiers) and versions wrappers
     * @return Minimum versions for the cluster
     */
    public static CompatibilityVersions minimumVersions(Map<String, CompatibilityVersions> compatibilityVersions) {
        return new CompatibilityVersions(
            compatibilityVersions.values()
                .stream()
                .map(CompatibilityVersions::transportVersion)
                .min(Comparator.naturalOrder())
                // In practice transportVersions is always nonempty (except in tests) but use a conservative default anyway:
                .orElse(TransportVersions.MINIMUM_COMPATIBLE)
        );
    }

    public static CompatibilityVersions readVersion(StreamInput in) throws IOException {
        return new CompatibilityVersions(TransportVersion.readVersion(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportVersion.writeVersion(this.transportVersion(), out);
    }

    /**
     * Adds fields to the builder without starting an object. We expect this method to be called within an object that may
     * already have a nodeId field.
     * @param builder The builder for the XContent
     * @param params Ignored here.
     * @return The builder with fields for versions added
     * @throws IOException if the builder can't accept what we try to add
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("transport_version", this.transportVersion().toString());
        return builder;
    }
}
