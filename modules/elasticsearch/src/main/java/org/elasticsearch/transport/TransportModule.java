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

package org.elasticsearch.transport;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.elasticsearch.util.Classes;
import org.elasticsearch.util.settings.Settings;

import static org.elasticsearch.util.guice.ModulesFactory.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportModule extends AbstractModule {

    private final Settings settings;

    public TransportModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(TransportService.class).asEagerSingleton();
        bind(TransportServiceManagement.class).asEagerSingleton();

        Class<? extends Module> defaultTransportModule = null;
        try {
            Classes.getDefaultClassLoader().loadClass("org.elasticsearch.transport.netty.NettyTransport");
            defaultTransportModule = (Class<? extends Module>) Classes.getDefaultClassLoader().loadClass("org.elasticsearch.transport.netty.NettyTransportModule");
        } catch (ClassNotFoundException e) {
            // TODO default to the local one
        }

        Class<? extends Module> moduleClass = settings.getAsClass("transport.type", defaultTransportModule, "org.elasticsearch.transport.", "TransportModule");
        createModule(moduleClass, settings).configure(binder());
    }
}