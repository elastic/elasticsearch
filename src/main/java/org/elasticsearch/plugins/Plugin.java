/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.plugins;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.CloseableIndexComponent;

import java.util.Collection;

/**
 * An extension point allowing to plug in custom functionality.
 *
 * @author kimchy (shay.banon)
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
    Collection<Class<? extends Module>> modules();

    /**
     * Node level services that will be automatically started/stopped/closed.
     */
    Collection<Class<? extends LifecycleComponent>> services();

    /**
     * Per index modules.
     */
    Collection<Class<? extends Module>> indexModules();

    /**
     * Per index services that will be automatically closed.
     */
    Collection<Class<? extends CloseableIndexComponent>> indexServices();

    /**
     * Per index shard module.
     */
    Collection<Class<? extends Module>> shardModules();

    /**
     * Per index shard service that will be automatically closed.
     */
    Collection<Class<? extends CloseableIndexComponent>> shardServices();

    void processModule(Module module);

    /**
     * Additional node settings loaded by the plugin
     */
    Settings additionalSettings();
}
