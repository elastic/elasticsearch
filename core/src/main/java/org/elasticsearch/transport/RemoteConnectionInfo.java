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
import java.util.List;
import java.util.Objects;

/**
 * This class encapsulates all remote cluster information to be rendered on
 * <tt>_remote/info</tt> requests.
 */
public final class RemoteConnectionInfo implements ToXContentFragment, Writeable {
    final List<TransportAddress> seedNodes;
    final List<TransportAddress> httpAddresses;
    final int connectionsPerCluster;
    final TimeValue initialConnectionTimeout;
    final int numNodesConnected;
    final String clusterAlias;
    final boolean skipUnavailable;

    RemoteConnectionInfo(String clusterAlias, List<TransportAddress> seedNodes,
                         List<TransportAddress> httpAddresses,
                         int connectionsPerCluster, int numNodesConnected,
                         TimeValue initialConnectionTimeout, boolean skipUnavailable) {
        this.clusterAlias = clusterAlias;
        this.seedNodes = seedNodes;
        this.httpAddresses = httpAddresses;
        this.connectionsPerCluster = connectionsPerCluster;
        this.numNodesConnected = numNodesConnected;
        this.initialConnectionTimeout = initialConnectionTimeout;
        this.skipUnavailable = skipUnavailable;
    }

    public RemoteConnectionInfo(StreamInput input) throws IOException {
        seedNodes = input.readList(TransportAddress::new);
        httpAddresses = input.readList(TransportAddress::new);
        connectionsPerCluster = input.readVInt();
        initialConnectionTimeout = new TimeValue(input);
        numNodesConnected = input.readVInt();
        clusterAlias = input.readString();
        if (input.getVersion().onOrAfter(Version.V_6_1_0)) {
            skipUnavailable = input.readBoolean();
        } else {
            skipUnavailable = false;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(clusterAlias);
        {
            builder.startArray("seeds");
            for (TransportAddress addr : seedNodes) {
                builder.value(addr.toString());
            }
            builder.endArray();
            builder.startArray("http_addresses");
            for (TransportAddress addr : httpAddresses) {
                builder.value(addr.toString());
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(seedNodes);
        out.writeList(httpAddresses);
        out.writeVInt(connectionsPerCluster);
        initialConnectionTimeout.writeTo(out);
        out.writeVInt(numNodesConnected);
        out.writeString(clusterAlias);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeBoolean(skipUnavailable);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteConnectionInfo that = (RemoteConnectionInfo) o;
        return connectionsPerCluster == that.connectionsPerCluster &&
            numNodesConnected == that.numNodesConnected &&
            Objects.equals(seedNodes, that.seedNodes) &&
            Objects.equals(httpAddresses, that.httpAddresses) &&
            Objects.equals(initialConnectionTimeout, that.initialConnectionTimeout) &&
            Objects.equals(clusterAlias, that.clusterAlias) &&
            skipUnavailable == that.skipUnavailable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seedNodes, httpAddresses, connectionsPerCluster, initialConnectionTimeout,
                numNodesConnected, clusterAlias, skipUnavailable);
    }
}
