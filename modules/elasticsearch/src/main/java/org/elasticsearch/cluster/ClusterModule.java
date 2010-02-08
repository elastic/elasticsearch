/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster;

import com.google.inject.AbstractModule;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.MetaDataService;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.strategy.DefaultShardsRoutingStrategy;
import org.elasticsearch.cluster.routing.strategy.ShardsRoutingStrategy;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class ClusterModule extends AbstractModule {

    private final Settings settings;

    public ClusterModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(ShardsRoutingStrategy.class)
                .to(settings.getAsClass("cluster.routing.shards.type", DefaultShardsRoutingStrategy.class))
                .asEagerSingleton();

        bind(ClusterService.class).to(DefaultClusterService.class).asEagerSingleton();
        bind(MetaDataService.class).asEagerSingleton();
        bind(RoutingService.class).asEagerSingleton();

        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeIndexCreatedAction.class).asEagerSingleton();
        bind(NodeIndexDeletedAction.class).asEagerSingleton();
    }
}