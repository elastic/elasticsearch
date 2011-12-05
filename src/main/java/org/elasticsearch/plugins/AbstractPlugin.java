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

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.CloseableIndexComponent;

import java.util.Collection;

/**
 * A base class for a plugin.
 *
 * @author kimchy (shay.banon)
 */
public abstract class AbstractPlugin implements Plugin {

    /**
     * Defaults to return an empty list.
     */
    @Override public Collection<Class<? extends Module>> modules() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override public Collection<Class<? extends LifecycleComponent>> services() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override public Collection<Class<? extends Module>> indexModules() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override public Collection<Class<? extends CloseableIndexComponent>> indexServices() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override public Collection<Class<? extends Module>> shardModules() {
        return ImmutableList.of();
    }

    /**
     * Defaults to return an empty list.
     */
    @Override public Collection<Class<? extends CloseableIndexComponent>> shardServices() {
        return ImmutableList.of();
    }

    @Override public void processModule(Module module) {
        // nothing to do here
    }

    @Override public Settings additionalSettings() {
        return ImmutableSettings.Builder.EMPTY_SETTINGS;
    }

}
