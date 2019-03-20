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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
        seedNodes = Arrays.asList(input.readStringArray());
        connectionsPerCluster = input.readVInt();
        initialConnectionTimeout = input.readTimeValue();
        numNodesConnected = input.readVInt();
        clusterAlias = input.readString();
        skipUnavailable = input.readBoolean();
    }

    public List<String> getSeedNodes() {
        return seedNodes;
    }

    public int getConnectionsPerCluster() {
        return connectionsPerCluster;
    }

    public TimeValue getInitialConnectionTimeout() {
        return initialConnectionTimeout;
    }

    public int getNumNodesConnected() {
        return numNodesConnected;
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    public boolean isSkipUnavailable() {
        return skipUnavailable;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(seedNodes.toArray(new String[0]));
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
