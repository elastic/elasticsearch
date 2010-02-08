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

package org.elasticsearch.http;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.elasticsearch.http.action.HttpActionModule;
import org.elasticsearch.util.Classes;
import org.elasticsearch.util.settings.Settings;

import static org.elasticsearch.util.guice.ModulesFactory.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpServerModule extends AbstractModule {

    private final Settings settings;

    public HttpServerModule(Settings settings) {
        this.settings = settings;
    }

    @SuppressWarnings({"unchecked"}) @Override protected void configure() {
        bind(HttpServer.class).asEagerSingleton();

        Class<? extends Module> defaultHttpServerTransportModule = null;
        try {
            Classes.getDefaultClassLoader().loadClass("org.elasticsearch.http.netty.NettyHttpServerTransport");
            defaultHttpServerTransportModule = (Class<? extends Module>) Classes.getDefaultClassLoader().loadClass("org.elasticsearch.http.netty.NettyHttpServerTransportModule");
        } catch (ClassNotFoundException e) {
            // no netty one, ok...
            if (settings.get("http.type") == null) {
                // no explicit one is configured, bail
                return;
            }
        }

        Class<? extends Module> moduleClass = settings.getAsClass("http.type", defaultHttpServerTransportModule, "org.elasticsearch.http.", "HttpServerTransportModule");
        createModule(moduleClass, settings).configure(binder());

        new HttpActionModule().configure(binder());
    }
}
