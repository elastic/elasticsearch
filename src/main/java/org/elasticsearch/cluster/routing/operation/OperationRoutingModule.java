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

package org.elasticsearch.cluster.routing.operation;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.routing.operation.hash.HashFunction;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.cluster.routing.operation.plain.PlainOperationRoutingModule;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.common.inject.Modules.createModule;

/**
 *
 */
public class OperationRoutingModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public OperationRoutingModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(createModule(settings.getAsClass("cluster.routing.operation.type", PlainOperationRoutingModule.class, "org.elasticsearch.cluster.routing.operation.", "OperationRoutingModule"), settings));
    }

    @Override
    protected void configure() {
        bind(HashFunction.class).to(settings.getAsClass("cluster.routing.operation.hash.type", DjbHashFunction.class, "org.elasticsearch.cluster.routing.operation.hash.", "HashFunction")).asEagerSingleton();
    }
}
