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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.util.Collection;

/**
 * A base class for a plugin.
 * <p/>
 * A plugin can be dynamically injected with {@link Module} by implementing <tt>onModule(AnyModule)</tt> method
 * removing the need to override {@link #processModule(org.elasticsearch.common.inject.Module)} and check using
 * instanceof.
 */
public abstract class AbstractPlugin implements Plugin {

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Class<? extends Module>> modules() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Module> modules(Settings settings) {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Class<? extends Module>> indexModules() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Module> indexModules(Settings settings) {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Class<? extends Closeable>> indexServices() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Class<? extends Module>> shardModules() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Module> shardModules(Settings settings) {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override
    public Collection<Class<? extends Closeable>> shardServices() {
        return ImmutableList.of();
    }

    @Override
    public void processModule(Module module) {
        // nothing to do here
    }

    @Override
    public Settings additionalSettings() {
        return ImmutableSettings.Builder.EMPTY_SETTINGS;
    }

}
