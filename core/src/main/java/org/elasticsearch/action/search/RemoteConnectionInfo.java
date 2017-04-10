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
package org.elasticsearch.action.search;

import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;

/**
 * This class encapsulates all remote cluster information to be rendered on
 * <tt>_remote/info</tt> requests.
 */
public final class RemoteConnectionInfo implements ToXContent {
    final Collection<TransportAddress> seedNodes;
    final Collection<TransportAddress> httpAddresses;
    final int connectionsPerCluster;
    final TimeValue initialConnectionTimeout;
    final int numNodesConnected;
    final String clusterAlias;

    RemoteConnectionInfo(String clusterAlias, Collection<TransportAddress> seedNodes,
                         Collection<TransportAddress> httpAddresses,
                         int connectionsPerCluster, int numNodesConnected,
                         TimeValue initialConnectionTimeout) {
        this.clusterAlias = clusterAlias;
        this.seedNodes = seedNodes;
        this.httpAddresses = httpAddresses;
        this.connectionsPerCluster = connectionsPerCluster;
        this.numNodesConnected = numNodesConnected;
        this.initialConnectionTimeout = initialConnectionTimeout;
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
        }
        builder.endObject();
        return builder;
    }
}
