/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
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

/**
 * This class encapsulates all remote cluster information to be rendered on
 * {@code _remote/info} requests.
 */
public final class RemoteConnectionInfo implements ToXContentFragment, Writeable {

    final ModeInfo modeInfo;
    final TimeValue initialConnectionTimeout;
    final String clusterAlias;
    final boolean skipUnavailable;

    public RemoteConnectionInfo(String clusterAlias, ModeInfo modeInfo, TimeValue initialConnectionTimeout, boolean skipUnavailable) {
        this.clusterAlias = clusterAlias;
        this.modeInfo = modeInfo;
        this.initialConnectionTimeout = initialConnectionTimeout;
        this.skipUnavailable = skipUnavailable;
    }

    public RemoteConnectionInfo(StreamInput input) throws IOException {
        if (input.getVersion().onOrAfter(Version.V_7_6_0)) {
            RemoteConnectionStrategy.ConnectionStrategy mode = input.readEnum(RemoteConnectionStrategy.ConnectionStrategy.class);
            modeInfo = mode.getReader().read(input);
            initialConnectionTimeout = input.readTimeValue();
            clusterAlias = input.readString();
            skipUnavailable = input.readBoolean();
        } else {
            List<String> seedNodes = Arrays.asList(input.readStringArray());
            int connectionsPerCluster = input.readVInt();
            initialConnectionTimeout = input.readTimeValue();
            int numNodesConnected = input.readVInt();
            clusterAlias = input.readString();
            skipUnavailable = input.readBoolean();
            modeInfo = new SniffConnectionStrategy.SniffModeInfo(seedNodes, connectionsPerCluster, numNodesConnected);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
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
            && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modeInfo, initialConnectionTimeout, clusterAlias, skipUnavailable);
    }

    public interface ModeInfo extends ToXContentFragment, Writeable {

        boolean isConnected();

        String modeName();

        RemoteConnectionStrategy.ConnectionStrategy modeType();
    }
}
