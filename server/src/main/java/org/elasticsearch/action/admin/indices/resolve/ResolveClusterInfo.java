/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class ResolveClusterInfo implements Writeable {

    private final boolean connected;
    private final Boolean skipUnavailable;  // remote clusters don't know their setting, so they put null and querying cluster fills in
    private final Boolean matchingIndices;  // null means 'unknown' when not connected
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

    public ResolveClusterInfo(ResolveClusterInfo copyFrom, boolean skipUnavailable) {
        this(copyFrom.isConnected(), skipUnavailable, copyFrom.getMatchingIndices(), copyFrom.getBuild(), copyFrom.getError());
    }

    private ResolveClusterInfo(boolean connected, Boolean skipUnavailable, Boolean matchingIndices, Build build, String error) {
        this.connected = connected;
        this.skipUnavailable = skipUnavailable;
        this.matchingIndices = matchingIndices;
        this.build = build;
        this.error = error;
        System.err.printf("ERROR: %s ; matchingIndices: %s; connected: %s\n", error, matchingIndices, connected);
        assert error != null || matchingIndices != null || connected == false : "If matchingIndices is null, connected must be false";
    }

    public ResolveClusterInfo(StreamInput in) throws IOException {
        if (in.getTransportVersion().before(TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least Transport Version "
                    + TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED
                    + " but was "
                    + in.getTransportVersion()
            );
        }
        this.connected = in.readBoolean();
        this.skipUnavailable = in.readOptionalBoolean();
        this.matchingIndices = in.readOptionalBoolean();
        this.error = in.readOptionalString();
        if (error == null) {
            this.build = Build.readBuild(in);
        } else {
            this.build = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least Transport Version "
                    + TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED
                    + " but was "
                    + out.getTransportVersion()
            );
        }
        out.writeBoolean(connected);
        out.writeOptionalBoolean(skipUnavailable);
        out.writeOptionalBoolean(matchingIndices);
        out.writeOptionalString(error);
        if (build != null) {
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
}
