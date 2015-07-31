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

package org.elasticsearch.plugins;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;

/** A site-only plugin, just serves resources */
final class SitePlugin implements Plugin {
    final String name;
    final String description;
    
    SitePlugin(String name, String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return Collections.emptyList();
    }

    @Override
    public Collection<? extends Module> modules(Settings settings) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        return Collections.emptyList();
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        return Collections.emptyList();
    }

    @Override
    public Collection<? extends Module> indexModules(Settings settings) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Class<? extends Closeable>> indexServices() {
        return Collections.emptyList();
    }

    @Override
    public Collection<Class<? extends Module>> shardModules() {
        return Collections.emptyList();
    }

    @Override
    public Collection<? extends Module> shardModules(Settings settings) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Class<? extends Closeable>> shardServices() {
        return Collections.emptyList();
    }

    @Override
    public void processModule(Module module) {
    }

    @Override
    public Settings additionalSettings() {
        return Settings.EMPTY;
    }
}
