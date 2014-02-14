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

package org.elasticsearch.river;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.cluster.RiverClusterService;
import org.elasticsearch.river.routing.RiversRouter;

import java.util.Map;

/**
 *
 */
public class RiversModule extends AbstractModule {

    private final Settings settings;

    private Map<String, Class<? extends Module>> riverTypes = Maps.newHashMap();

    public RiversModule(Settings settings) {
        this.settings = settings;
    }

    /**
     * Registers a custom river type name against a module.
     *
     * @param type   The type
     * @param module The module
     */
    public void registerRiver(String type, Class<? extends Module> module) {
        riverTypes.put(type, module);
    }

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(RiverIndexName.class).toInstance(RiverIndexName.Conf.indexName(settings));
        bind(RiversService.class).asEagerSingleton();
        bind(RiverClusterService.class).asEagerSingleton();
        bind(RiversRouter.class).asEagerSingleton();
        bind(RiversManager.class).asEagerSingleton();
        bind(RiversTypesRegistry.class).toInstance(new RiversTypesRegistry(ImmutableMap.copyOf(riverTypes)));
    }
}
