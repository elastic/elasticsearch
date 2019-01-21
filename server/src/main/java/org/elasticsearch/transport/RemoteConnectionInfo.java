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
import org.elasticsearch.common.collect.Tuple;
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
    final List<String> seedNodes;
    final int connectionsPerCluster;
    final TimeValue initialConnectionTimeout;
    final int numNodesConnected;
    final String clusterAlias;
    final boolean skipUnavailable;

    RemoteConnectionInfo(String clusterAlias, List<String> seedNodes,
                         int connectionsPerCluster, int numNodesConnected,
                         TimeValue initialConnectionTimeout, boolean skipUnavailable) {
        this.clusterAlias = clusterAlias;
        this.seedNodes = seedNodes;
        this.connectionsPerCluster = connectionsPerCluster;
        this.numNodesConnected = numNodesConnected;
        this.initialConnectionTimeout = initialConnectionTimeout;
        this.skipUnavailable = skipUnavailable;
    }

    public RemoteConnectionInfo(StreamInput input) throws IOException {
        if (input.getVersion().onOrAfter(Version.V_7_0_0)) {
            seedNodes = Arrays.asList(input.readStringArray());
        } else {
            // versions prior to 7.0.0 sent the resolved transport address of the seed nodes
            final List<TransportAddress> transportAddresses = input.readList(TransportAddress::new);
            seedNodes =
                    transportAddresses
                            .stream()
                            .map(a -> a.address().getHostString() + ":" + a.address().getPort())
                            .collect(Collectors.toList());
        }
        if (input.getVersion().before(Version.V_7_0_0)) {
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
        connectionsPerCluster = input.readVInt();
        initialConnectionTimeout = input.readTimeValue();
        numNodesConnected = input.readVInt();
        clusterAlias = input.readString();
        skipUnavailable = input.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeStringArray(seedNodes.toArray(new String[0]));
        } else {
            // versions prior to 7.0.0 received the resolved transport address of the seed nodes
            out.writeList(seedNodes
                    .stream()
                    .map(
                            s -> {
                                final Tuple<String, Integer> hostPort = RemoteClusterAware.parseHostPort(s);
                                assert hostPort.v2() != null : s;
                                try {
                                    return new TransportAddress(
                                            InetAddress.getByAddress(hostPort.v1(), TransportAddress.META_ADDRESS.getAddress()),
                                            hostPort.v2());
                                } catch (final UnknownHostException e) {
                                    throw new AssertionError(e);
                                }
                            })
                    .collect(Collectors.toList()));
        }
        if (out.getVersion().before(Version.V_7_0_0)) {
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
        out.writeVInt(connectionsPerCluster);
        out.writeTimeValue(initialConnectionTimeout);
        out.writeVInt(numNodesConnected);
        out.writeString(clusterAlias);
        out.writeBoolean(skipUnavailable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(clusterAlias);
        {
            builder.startArray("seeds");
            for (String addr : seedNodes) {
                builder.value(addr);
            }
            builder.endArray();
            builder.field("connected", numNodesConnected > 0);
            builder.field("num_nodes_connected", numNodesConnected);
            builder.field("max_connections_per_cluster", connectionsPerCluster);
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
        return connectionsPerCluster == that.connectionsPerCluster &&
            numNodesConnected == that.numNodesConnected &&
            Objects.equals(seedNodes, that.seedNodes) &&
            Objects.equals(initialConnectionTimeout, that.initialConnectionTimeout) &&
            Objects.equals(clusterAlias, that.clusterAlias) &&
            skipUnavailable == that.skipUnavailable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seedNodes, connectionsPerCluster, initialConnectionTimeout,
                numNodesConnected, clusterAlias, skipUnavailable);
    }

}
