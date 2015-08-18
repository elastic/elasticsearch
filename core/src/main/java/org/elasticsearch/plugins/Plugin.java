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

/**
 * An extension point allowing to plug in custom functionality.
 * <p/>
 * A plugin can be register custom extensions to builtin behavior by implementing <tt>onModule(AnyModule)</tt>,
 * and registering the extension with the given module.
 */
public interface Plugin {

    /**
     * The name of the plugin.
     */
    String name();

    /**
     * The description of the plugin.
     */
    String description();

    /**
     * Node level modules.
     */
    Collection<Module> nodeModules();

    /**
     * Node level services that will be automatically started/stopped/closed.
     */
    Collection<Class<? extends LifecycleComponent>> nodeServices();

    /**
     * Per index modules.
     */
    Collection<Module> indexModules();

    /**
     * Per index services that will be automatically closed.
     */
    Collection<Class<? extends Closeable>> indexServices();

    /**
     * Per index shard module.
     */
    Collection<Module> shardModules();

    /**
     * Per index shard service that will be automatically closed.
     */
    Collection<Class<? extends Closeable>> shardServices();

    /**
     * Additional node settings loaded by the plugin. Note that settings that are explicit in the nodes settings can't be
     * overwritten with the additional settings. These settings added if they don't exist.
     */
    Settings additionalSettings();
}
