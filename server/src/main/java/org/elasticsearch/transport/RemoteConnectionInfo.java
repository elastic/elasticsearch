/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class encapsulates all remote cluster information to be rendered on
 * {@code _remote/info} requests.
 */
public final class RemoteConnectionInfo implements ToXContentFragment, Writeable {
    private static final String CONNECTED = "connected";
    private static final String MODE = "mode";
    private static final String INITIAL_CONNECT_TIMEOUT = "initial_connect_timeout";
    private static final String SKIP_UNAVAILABLE = "skip_unavailable";

    private static final ConstructingObjectParser<RemoteConnectionInfo, String> PARSER = new ConstructingObjectParser<>(
            "RemoteConnectionInfoObjectParser",
            false,
            (args, clusterAlias) -> {
                String mode = (String) args[1];
                ModeInfo modeInfo;
                if (mode.equals(SimpleConnectionStrategy.SimpleModeInfo.NAME)) {
                    modeInfo = new SimpleConnectionStrategy.SimpleModeInfo((List<String>) args[4], (int) args[5], (int) args[6]);
                } else if (mode.equals(SniffConnectionStrategy.SniffModeInfo.NAME)) {
                    modeInfo = new SniffConnectionStrategy.SniffModeInfo((List<String>) args[7], (int) args[8], (int) args[9]);
                } else {
                    throw new IllegalArgumentException("mode cannot be " + mode);
                }
                return new RemoteConnectionInfo(clusterAlias,
                        modeInfo,
                        TimeValue.parseTimeValue((String) args[2], INITIAL_CONNECT_TIMEOUT),
                        (boolean) args[3]);
            });

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(CONNECTED));
        PARSER.declareString(constructorArg(), new ParseField(MODE));
        PARSER.declareString(constructorArg(), new ParseField(INITIAL_CONNECT_TIMEOUT));
        PARSER.declareBoolean(constructorArg(), new ParseField(SKIP_UNAVAILABLE));

        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(SimpleConnectionStrategy.SimpleModeInfo.ADDRESSES));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SimpleConnectionStrategy.SimpleModeInfo.MAX_SOCKET_CONNECTIONS));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SimpleConnectionStrategy.SimpleModeInfo.NUM_SOCKETS_CONNECTED));

        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(SniffConnectionStrategy.SniffModeInfo.SEEDS));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SniffConnectionStrategy.SniffModeInfo.MAX_CONNECTIONS_PER_CLUSTER));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SniffConnectionStrategy.SniffModeInfo.NUM_NODES_CONNECTED));
    }

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
        // TODO: Change to 7.6 after backport
        if (input.getVersion().onOrAfter(Version.V_8_0_0)) {
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
        // TODO: Change to 7.6 after backport
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
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
            builder.field(CONNECTED, modeInfo.isConnected());
            builder.field(MODE, modeInfo.modeName());
            modeInfo.toXContent(builder, params);
            builder.field(INITIAL_CONNECT_TIMEOUT, initialConnectionTimeout.getStringRep());
            builder.field(SKIP_UNAVAILABLE, skipUnavailable);
        }
        builder.endObject();
        return builder;
    }

    public static RemoteConnectionInfo fromXContent(XContentParser parser, String clusterAlias) throws IOException {
        return PARSER.parse(parser, clusterAlias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteConnectionInfo that = (RemoteConnectionInfo) o;
        return skipUnavailable == that.skipUnavailable &&
            Objects.equals(modeInfo, that.modeInfo) &&
            Objects.equals(initialConnectionTimeout, that.initialConnectionTimeout) &&
            Objects.equals(clusterAlias, that.clusterAlias);
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
