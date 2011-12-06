/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.monitor.dump.cluster;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.dump.Dump;
import org.elasticsearch.monitor.dump.DumpContributionFailedException;
import org.elasticsearch.monitor.dump.DumpContributor;

import java.io.PrintWriter;

/**
 *
 */
public class ClusterDumpContributor implements DumpContributor {

    public static final String CLUSTER = "cluster";

    private final String name;

    private final ClusterService clusterService;

    @Inject
    public ClusterDumpContributor(ClusterService clusterService, @Assisted String name, @Assisted Settings settings) {
        this.clusterService = clusterService;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void contribute(Dump dump) throws DumpContributionFailedException {
        ClusterState clusterState = clusterService.state();
        DiscoveryNodes nodes = clusterState.nodes();
        RoutingTable routingTable = clusterState.routingTable();

        PrintWriter writer = new PrintWriter(dump.createFileWriter("cluster.txt"));

        writer.println("===== CLUSTER NODES ======");
        writer.print(nodes.prettyPrint());

        writer.println("===== ROUTING TABLE ======");
        writer.print(routingTable.prettyPrint());

        writer.close();
    }
}
