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

package org.elasticsearch.transport;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.local.LocalTransportModule;
import org.elasticsearch.transport.netty.NettyTransportModule;

/**
 *
 */
public class TransportModule extends AbstractModule implements SpawnModules {

    private final Settings settings;
    
    public static final String TRANSPORT_TYPE_KEY = "transport.type";

    public TransportModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        Class<? extends Module> defaultTransportModule;
        if (DiscoveryNode.localNode(settings)) {
            defaultTransportModule = LocalTransportModule.class;
        } else {
            defaultTransportModule = NettyTransportModule.class;
        }
        return ImmutableList.of(Modules.createModule(settings.getAsClass(TRANSPORT_TYPE_KEY, defaultTransportModule, "org.elasticsearch.transport.", "TransportModule"), settings));
    }

    @Override
    protected void configure() {
        bind(TransportService.class).asEagerSingleton();
    }
}