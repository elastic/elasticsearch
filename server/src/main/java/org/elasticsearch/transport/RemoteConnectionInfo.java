/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS;

/**
 * This class encapsulates all remote cluster information to be rendered on
 * {@code _remote/info} requests.
 */
public final class RemoteConnectionInfo implements ToXContentFragment, Writeable {

    final ModeInfo modeInfo;
    final TimeValue initialConnectionTimeout;
    final String clusterAlias;
    final boolean skipUnavailable;
    final boolean hasClusterCredentials;

    public RemoteConnectionInfo(
        String clusterAlias,
        ModeInfo modeInfo,
        TimeValue initialConnectionTimeout,
        boolean skipUnavailable,
        boolean hasClusterCredentials
    ) {
        this.clusterAlias = clusterAlias;
        this.modeInfo = modeInfo;
        this.initialConnectionTimeout = initialConnectionTimeout;
        this.skipUnavailable = skipUnavailable;
        this.hasClusterCredentials = hasClusterCredentials;
    }

    public RemoteConnectionInfo(StreamInput input) throws IOException {
        if (input.getTransportVersion().onOrAfter(TransportVersion.V_7_6_0)) {
            RemoteConnectionStrategy.ConnectionStrategy mode = input.readEnum(RemoteConnectionStrategy.ConnectionStrategy.class);
            modeInfo = mode.getReader().read(input);
            initialConnectionTimeout = input.readTimeValue();
            clusterAlias = input.readString();
            skipUnavailable = input.readBoolean();
            if (input.getTransportVersion().onOrAfter(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS)) {
                hasClusterCredentials = input.readBoolean();
            } else {
                hasClusterCredentials = false;
            }
        } else {
            List<String> seedNodes = Arrays.asList(input.readStringArray());
            int connectionsPerCluster = input.readVInt();
            initialConnectionTimeout = input.readTimeValue();
            int numNodesConnected = input.readVInt();
            clusterAlias = input.readString();
            skipUnavailable = input.readBoolean();
            modeInfo = new SniffConnectionStrategy.SniffModeInfo(seedNodes, connectionsPerCluster, numNodesConnected);
            hasClusterCredentials = false;
        }
    }

    public boolean isConnected() {
        return modeInfo.isConnected();
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    public ModeInfo getModeInfo() {
        return modeInfo;
    }

    public TimeValue getInitialConnectionTimeout() {
        return initialConnectionTimeout;
    }

    public boolean isSkipUnavailable() {
        return skipUnavailable;
    }

    public boolean hasClusterCredentials() {
        return hasClusterCredentials;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_6_0)) {
            out.writeEnum(modeInfo.modeType());
            modeInfo.writeTo(out);
            out.writeTimeValue(initialConnectionTimeout);
        } else {
            if (modeInfo.modeType() == RemoteConnectionStrategy.ConnectionStrategy.SNIFF) {
                SniffConnectionStrategy.SniffModeInfo sniffInfo = (SniffConnectionStrategy.SniffModeInfo) this.modeInfo;
                out.writeStringArray(sniffInfo.seedNodes.toArray(new String[0]));
                out.writeVInt(sniffInfo.maxConnectionsPerCluster);
                out.writeTimeValue(initialConnectionTimeout);
                out.writeVInt(sniffInfo.numNodesConnected);
            } else {
                out.writeStringArray(new String[0]);
                out.writeVInt(0);
                out.writeTimeValue(initialConnectionTimeout);
                out.writeVInt(0);
            }
        }
        out.writeString(clusterAlias);
        out.writeBoolean(skipUnavailable);
        if (out.getTransportVersion().onOrAfter(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS)) {
            out.writeBoolean(hasClusterCredentials);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(clusterAlias);
        {
            builder.field("connected", modeInfo.isConnected());
            builder.field("mode", modeInfo.modeName());
            modeInfo.toXContent(builder, params);
            builder.field("initial_connect_timeout", initialConnectionTimeout);
            builder.field("skip_unavailable", skipUnavailable);
            if (hasClusterCredentials) {
                builder.field("cluster_credentials", "::es_redacted::");
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteConnectionInfo that = (RemoteConnectionInfo) o;
        return skipUnavailable == that.skipUnavailable
            && Objects.equals(modeInfo, that.modeInfo)
            && Objects.equals(initialConnectionTimeout, that.initialConnectionTimeout)
            && Objects.equals(clusterAlias, that.clusterAlias)
            && hasClusterCredentials == that.hasClusterCredentials;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modeInfo, initialConnectionTimeout, clusterAlias, skipUnavailable, hasClusterCredentials);
    }

    public interface ModeInfo extends ToXContentFragment, Writeable {

        boolean isConnected();

        String modeName();

        RemoteConnectionStrategy.ConnectionStrategy modeType();
    }
}
