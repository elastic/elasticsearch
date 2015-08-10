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

package org.elasticsearch.plugin.example;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesModule;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class JvmExamplePlugin implements Plugin {

    private final Settings settings;

    public JvmExamplePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "jvm-example";
    }

    @Override
    public String description() {
        return "A plugin that extends all extension points";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return Collections.emptyList();
    }

    @Override
    public Collection<Module> modules(Settings settings) {
        Collection<Module> modules = new ArrayList<>();
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        return services;
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

    public void onModule(RepositoriesModule repositoriesModule) {
    }


}
