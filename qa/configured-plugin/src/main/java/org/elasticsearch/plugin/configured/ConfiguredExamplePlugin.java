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

package org.elasticsearch.plugin.configured;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.action.cat.AbstractCatAction;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Example configured plugin.
 */
public class ConfiguredExamplePlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "configured-example";
    }

    @Override
    public String description() {
        return "Example of a configured plugin";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> classes = new ArrayList<>();
        classes.add(ConfiguredExampleModule.class);
        return classes;
    }

    public static class ConfiguredExampleModule extends AbstractModule {
        @Override
        protected void configure() {
          bind(ConfiguredExamplePluginConfiguration.class).asEagerSingleton();
          Multibinder<AbstractCatAction> catActionMultibinder = Multibinder.newSetBinder(binder(), AbstractCatAction.class);
          catActionMultibinder.addBinding().to(ConfiguredExampleCatAction.class).asEagerSingleton();
        }
    }
}
