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

package org.elasticsearch.monitor.process;

import com.google.common.collect.Maps;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

public class ProcessModule extends AbstractModule {

    private Map<String, Class<? extends ProcessProbe>> probes = Maps.newHashMap();

    public void registerProbe(String type, Class<? extends ProcessProbe> probe) {
        probes.put(type, probe);
    }

    @Override
    protected void configure() {
        MapBinder<String, ProcessProbe> mbinder = MapBinder.newMapBinder(binder(), String.class, ProcessProbe.class);

        // Bind default probe
        bind(JmxProcessProbe.class).asEagerSingleton();
        mbinder.addBinding(JmxProcessProbe.TYPE).to(JmxProcessProbe.class);

        for (Map.Entry<String, Class<? extends ProcessProbe>> entry : probes.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }
        bind(ProcessProbeRegistry.class).asEagerSingleton();
        bind(ProcessService.class).asEagerSingleton();
    }
}