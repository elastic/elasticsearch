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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class ResolveClusterInfo implements Writeable {

    private final boolean connected;
    private final Boolean skipUnavailable;  // remote clusters don't know their setting, so they put null and querying cluster fills in
    private final Boolean matchingIndices;  // null means no index expression requested by user or remote cluster was not connected
    private final Build build;
    private final String error;

    public ResolveClusterInfo(boolean connected, Boolean skipUnavailable) {
        this(connected, skipUnavailable, null, null, null);
    }

    public ResolveClusterInfo(boolean connected, Boolean skipUnavailable, String error) {
        this(connected, skipUnavailable, null, null, error);
    }

    public ResolveClusterInfo(boolean connected, Boolean skipUnavailable, Boolean matchingIndices, Build build) {
        this(connected, skipUnavailable, matchingIndices, build, null);
    }

    public ResolveClusterInfo(ResolveClusterInfo copyFrom, boolean skipUnavailable, boolean clusterInfoOnly) {
        this(
            copyFrom.isConnected(),
            skipUnavailable,
            clusterInfoOnly ? null : copyFrom.getMatchingIndices(),
            copyFrom.getBuild(),
            clusterInfoOnly ? null : copyFrom.getError()
        );
    }

    private ResolveClusterInfo(boolean connected, Boolean skipUnavailable, Boolean matchingIndices, Build build, String error) {
        this.connected = connected;
        this.skipUnavailable = skipUnavailable;
        this.matchingIndices = matchingIndices;
        this.build = build;
        this.error = error;
    }

    public ResolveClusterInfo(StreamInput in) throws IOException {
        this.connected = in.readBoolean();
        this.skipUnavailable = in.readOptionalBoolean();
        this.matchingIndices = in.readOptionalBoolean();
        this.error = in.readOptionalString();
        boolean buildIsPresent = in.readBoolean();
        if (buildIsPresent) {
            this.build = Build.readBuild(in);
        } else {
            this.build = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            throw new UnsupportedOperationException(ResolveClusterActionRequest.createVersionErrorMessage(out.getTransportVersion()));
        }
        out.writeBoolean(connected);
        out.writeOptionalBoolean(skipUnavailable);
        out.writeOptionalBoolean(matchingIndices);
        out.writeOptionalString(error);
        // since TransportResolveClusterAction has fallbacks, Build is not always present and doesn't have a "writeOptional" method
        // so we add an extra boolean to specify whether build has been serialized or not
        boolean buildIsPresent = build != null;
        out.writeBoolean(buildIsPresent);
        if (buildIsPresent) {
            Build.writeBuild(build, out);
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public Boolean getSkipUnavailable() {
        return skipUnavailable;
    }

    public Boolean getMatchingIndices() {
        return matchingIndices;
    }

    public Build getBuild() {
        return build;
    }

    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        return "ResolveClusterInfo{"
            + "connected="
            + connected
            + ", skipUnavailable="
            + skipUnavailable
            + ", matchingIndices="
            + matchingIndices
            + ", build="
            + build
            + ", error="
            + error
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResolveClusterInfo that = (ResolveClusterInfo) o;
        return connected == that.connected
            && Objects.equals(skipUnavailable, that.skipUnavailable)
            && Objects.equals(matchingIndices, that.matchingIndices)
            && Objects.equals(build, that.build)
            && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connected, skipUnavailable, matchingIndices, build, error);
    }
}
