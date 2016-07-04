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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.rest.action.cat.AbstractCatAction;

/**
 * Example of a plugin.
 */
public class JvmExamplePlugin extends Plugin {

    private final Settings settings;

    public JvmExamplePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.<Module>singletonList(new ConfiguredExampleModule());
    }

    @Override
    @SuppressWarnings("rawtypes") // Plugin use a rawtype
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        return services;
    }

    @Override
    public Settings additionalSettings() {
        return Settings.EMPTY;
    }

    public void onModule(RepositoriesModule repositoriesModule) {
    }

    /**
     * Module decalaring some example configuration and a _cat action that uses
     * it.
     */
    public static class ConfiguredExampleModule extends AbstractModule {
        @Override
        protected void configure() {
          bind(ExamplePluginConfiguration.class).asEagerSingleton();
          Multibinder<AbstractCatAction> catActionMultibinder = Multibinder.newSetBinder(binder(), AbstractCatAction.class);
          catActionMultibinder.addBinding().to(ExampleCatAction.class).asEagerSingleton();
        }
    }
}
