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

public final class RemoteConnectionInfo implements ToXContent {
    final Collection<TransportAddress> seedNodes;
    final Collection<TransportAddress> httpAddresses;
    final int connectionsPerCluster;
    final boolean connected;
    final TimeValue initialConnectionTimeout;
    final String clusterAlias;

    RemoteConnectionInfo(String clusterAlias, Collection<TransportAddress> seedNodes,
                         Collection<TransportAddress> httpAddresses,
                         int connectionsPerCluster, boolean connected,
                         TimeValue initialConnectionTimeout) {
        this.clusterAlias = clusterAlias;
        this.seedNodes = seedNodes;
        this.httpAddresses = httpAddresses;
        this.connectionsPerCluster = connectionsPerCluster;
        this.connected = connected;
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
            builder.field("connected", connected);
            builder.field("connections_per_cluster", connectionsPerCluster);
            builder.field("initial_connect_timeout", initialConnectionTimeout);
        }
        builder.endObject();
        return builder;
    }
}
