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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

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
            List<String> seedNodes;
            if (input.getVersion().onOrAfter(Version.V_7_0_0)) {
                seedNodes = Arrays.asList(input.readStringArray());
            } else {
                // versions prior to 7.0.0 sent the resolved transport address of the seed nodes
                final List<TransportAddress> transportAddresses = input.readList(TransportAddress::new);
                seedNodes = transportAddresses
                    .stream()
                    .map(a -> a.address().getHostString() + ":" + a.address().getPort())
                    .collect(Collectors.toList());
                /*
                 * Versions before 7.0 sent the HTTP addresses of all nodes in the
                 * remote cluster here but it was expensive to fetch and we
                 * ultimately figured out how to do without it. So we removed it.
                 *
                 * We just throw any HTTP addresses received here on the floor
                 * because we don't need to do anything with them.
                 */
                input.readList(TransportAddress::new);
            }

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
                if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
                    out.writeStringArray(sniffInfo.seedNodes.toArray(new String[0]));
                } else {
                    // versions prior to 7.0.0 received the resolved transport address of the seed nodes
                    out.writeList(sniffInfo.seedNodes
                        .stream()
                        .map(
                            s -> {
                                final String host = RemoteConnectionStrategy.parseHost(s);
                                final int port = RemoteConnectionStrategy.parsePort(s);
                                try {
                                    return new TransportAddress(
                                        InetAddress.getByAddress(host, TransportAddress.META_ADDRESS.getAddress()), port);
                                } catch (final UnknownHostException e) {
                                    throw new AssertionError(e);
                                }
                            })
                        .collect(Collectors.toList()));
                    /*
                     * Versions before 7.0 sent the HTTP addresses of all nodes in the
                     * remote cluster here but it was expensive to fetch and we
                     * ultimately figured out how to do without it. So we removed it.
                     *
                     * When sending this request to a node that expects HTTP addresses
                     * here we pretend that we didn't find any. This *should* be fine
                     * because, after all, we haven't been using this information for
                     * a while.
                     */
                    out.writeList(emptyList());
                }
                out.writeVInt(sniffInfo.maxConnectionsPerCluster);
                out.writeTimeValue(initialConnectionTimeout);
                out.writeVInt(sniffInfo.numNodesConnected);
            } else {
                if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
                    out.writeStringArray(new String[0]);
                } else {
                    // versions prior to 7.0.0 received the resolved transport address of the seed nodes
                    out.writeList(emptyList());
                    /*
                     * Versions before 7.0 sent the HTTP addresses of all nodes in the
                     * remote cluster here but it was expensive to fetch and we
                     * ultimately figured out how to do without it. So we removed it.
                     *
                     * When sending this request to a node that expects HTTP addresses
                     * here we pretend that we didn't find any. This *should* be fine
                     * because, after all, we haven't been using this information for
                     * a while.
                     */
                    out.writeList(emptyList());
                }
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
