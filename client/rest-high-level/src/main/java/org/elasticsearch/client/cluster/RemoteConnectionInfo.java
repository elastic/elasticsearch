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

package org.elasticsearch.client.cluster;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class encapsulates all remote cluster information to be rendered on
 * {@code _remote/info} requests.
 */
public final class RemoteConnectionInfo {
    private static final String CONNECTED = "connected";
    private static final String MODE = "mode";
    private static final String INITIAL_CONNECT_TIMEOUT = "initial_connect_timeout";
    private static final String SKIP_UNAVAILABLE = "skip_unavailable";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RemoteConnectionInfo, String> PARSER = new ConstructingObjectParser<>(
            "RemoteConnectionInfoObjectParser",
            false,
            (args, clusterAlias) -> {
                String mode = (String) args[1];
                ModeInfo modeInfo;
                if (mode.equals(ProxyModeInfo.NAME)) {
                    modeInfo = new ProxyModeInfo((String) args[4], (int) args[5], (int) args[6]);
                } else if (mode.equals(SniffModeInfo.NAME)) {
                    modeInfo = new SniffModeInfo((List<String>) args[7], (int) args[8], (int) args[9]);
                } else {
                    throw new IllegalArgumentException("mode cannot be " + mode);
                }
                return new RemoteConnectionInfo(clusterAlias,
                        modeInfo,
                        (String) args[2],
                        (boolean) args[3]);
            });

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(CONNECTED));
        PARSER.declareString(constructorArg(), new ParseField(MODE));
        PARSER.declareString(constructorArg(), new ParseField(INITIAL_CONNECT_TIMEOUT));
        PARSER.declareBoolean(constructorArg(), new ParseField(SKIP_UNAVAILABLE));

        PARSER.declareString(optionalConstructorArg(), new ParseField(ProxyModeInfo.ADDRESS));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(ProxyModeInfo.MAX_SOCKET_CONNECTIONS));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(ProxyModeInfo.NUM_SOCKETS_CONNECTED));

        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(SniffModeInfo.SEEDS));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SniffModeInfo.MAX_CONNECTIONS_PER_CLUSTER));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SniffModeInfo.NUM_NODES_CONNECTED));
    }

    private final ModeInfo modeInfo;
    // TODO: deprecate and remove this field in favor of initialConnectionTimeout field that is of type TimeValue.
    // When rest api versioning exists then change org.elasticsearch.transport.RemoteConnectionInfo to properly serialize
    // the initialConnectionTimeout field so that we can properly parse initialConnectionTimeout as TimeValue
    private final String initialConnectionTimeoutString;
    private final String clusterAlias;
    private final boolean skipUnavailable;

    RemoteConnectionInfo(String clusterAlias, ModeInfo modeInfo, String initialConnectionTimeoutString, boolean skipUnavailable) {
        this.clusterAlias = clusterAlias;
        this.modeInfo = modeInfo;
        this.initialConnectionTimeoutString = initialConnectionTimeoutString;
        this.skipUnavailable = skipUnavailable;
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

    public String getInitialConnectionTimeoutString() {
        return initialConnectionTimeoutString;
    }

    public boolean isSkipUnavailable() {
        return skipUnavailable;
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
                Objects.equals(initialConnectionTimeoutString, that.initialConnectionTimeoutString) &&
                Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modeInfo, initialConnectionTimeoutString, clusterAlias, skipUnavailable);
    }

    public interface ModeInfo {

        boolean isConnected();

        String modeName();
    }
}
