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

package org.elasticsearch.discovery;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.elasticsearch.util.Classes;
import org.elasticsearch.util.settings.Settings;

import static org.elasticsearch.util.guice.ModulesFactory.*;

/**
 * @author kimchy (Shay Banon)
 */
public class DiscoveryModule extends AbstractModule {

    private final Settings settings;

    public DiscoveryModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        Class<? extends Module> defaultDiscoveryModule = null;
        try {
            Classes.getDefaultClassLoader().loadClass("org.elasticsearch.discovery.jgroups.JgroupsDiscovery");
            defaultDiscoveryModule = (Class<? extends Module>) Classes.getDefaultClassLoader().loadClass("org.elasticsearch.discovery.jgroups.JgroupsDiscoveryModule");
        } catch (ClassNotFoundException e) {
            // TODO default to the local one
        }

        Class<? extends Module> moduleClass = settings.getAsClass("discovery.type", defaultDiscoveryModule, "org.elasticsearch.discovery.", "DiscoveryModule");
        createModule(moduleClass, settings).configure(binder());

        bind(DiscoveryService.class).asEagerSingleton();
    }
}