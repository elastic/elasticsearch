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

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.collect.ImmutableOpenMap;

/**
 */
public class ClusterStateParts extends CompositeClusterStatePart<ClusterStateParts> {
    public static final String TYPE = "cluster";

    public static final Factory FACTORY = new Factory();

    public static class Factory extends AbstractCompositeClusterStatePartFactory<ClusterStateParts> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public ClusterStateParts fromParts(ImmutableOpenMap.Builder<String, ClusterStatePart> parts) {
            return new ClusterStateParts(parts.build());
        }
    }

    static {
        registerFactory(DiscoveryNodes.TYPE, DiscoveryNodes.FACTORY);
        registerFactory(ClusterBlocks.TYPE, ClusterBlocks.FACTORY);
        registerFactory(RoutingTable.TYPE, RoutingTable.FACTORY);
        registerFactory(MetaData.TYPE, MetaData.FACTORY);
    }

    @Override
    public String type() {
        return TYPE;
    }

    static {
        registerFactory(DiscoveryNodes.TYPE, DiscoveryNodes.FACTORY);
        registerFactory(ClusterBlocks.TYPE, ClusterBlocks.FACTORY);
        registerFactory(RoutingTable.TYPE, RoutingTable.FACTORY);
        registerFactory(MetaData.TYPE, MetaData.FACTORY);
    }

    public ClusterStateParts(ImmutableOpenMap<String, ClusterStatePart> parts) {
        super(parts);
    }

}