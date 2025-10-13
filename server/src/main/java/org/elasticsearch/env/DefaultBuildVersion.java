/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link BuildVersion} that uses the same identifiers and compatibility constraints
 * as {@link Version}.
 *
 * <p>This default implementation of BuildVersion delegates to the {@link Version} class.
 * It's intended to let us check wither a version identifier is "too old" or "too new."
 * "Too old" is determined by {@code Version.CURRENT.minimumCompatibilityVersion()},
 * and "too new" is anything that comes after {@code Version.CURRENT}. This lets us
 * give users simple rules in terms of public-facing release versions for Elasticsearch
 * compatibility when upgrading nodes and prevents downgrades in place.</p>
 */
final class DefaultBuildVersion extends BuildVersion {

    public static final BuildVersion CURRENT = new DefaultBuildVersion(Version.CURRENT.id());

    final Version version;

    DefaultBuildVersion(int versionId) {
        assert versionId >= 0 : "Release version IDs must be non-negative integers";
        this.version = Version.fromId(versionId);
    }

    DefaultBuildVersion(String version) {
        this.version = Version.fromString(Objects.requireNonNull(version));
    }

    DefaultBuildVersion(StreamInput in) throws IOException {
        this(in.readVInt());
    }

    @Override
    public boolean canRemoveAssumedFeatures() {
        /*
         * We can remove assumed features if the node version is the next major version.
         * This is because the next major version can only form a cluster with the
         * latest minor version of the previous major, so any features introduced before that point
         * (that are marked as assumed in the running code version) are automatically met by that version.
         */
        return version.major == Version.CURRENT.major + 1;
    }

    @Override
    public boolean onOrAfterMinimumCompatible() {
        return Version.CURRENT.minimumCompatibilityVersion().onOrBefore(version);
    }

    @Override
    public boolean isFutureVersion() {
        return Version.CURRENT.before(version);
    }

    @Override
    public String toNodeMetadata() {
        return Integer.toString(version.id());
    }

    @Override
    public BuildVersion minimumCompatibilityVersion() {
        return fromVersionId(version.minimumCompatibilityVersion().id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(version.id());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(version.id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultBuildVersion that = (DefaultBuildVersion) o;
        return version.equals(that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version.id());
    }

    @Override
    public String toString() {
        return version.toString();
    }
}
