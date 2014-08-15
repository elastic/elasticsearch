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

package org.elasticsearch.http;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.plugins.Plugin;

import static org.elasticsearch.common.Preconditions.checkNotNull;

/**
 *
 */
public class HttpServerModule extends AbstractModule {

    private final Settings settings;
    private final ESLogger logger;

    private Class<? extends HttpServerTransport> configuredHttpServerTransport;
    private String configuredHttpServerTransportSource;

    public HttpServerModule(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    protected void configure() {
        if (configuredHttpServerTransport != null) {
            logger.info("Using [{}] as http transport, overridden by [{}]", configuredHttpServerTransport.getName(), configuredHttpServerTransportSource);
            bind(HttpServerTransport.class).to(configuredHttpServerTransport).asEagerSingleton();
        } else {
            Class<? extends HttpServerTransport> defaultHttpServerTransport = NettyHttpServerTransport.class;
            Class<? extends HttpServerTransport> httpServerTransport = settings.getAsClass("http.type", defaultHttpServerTransport, "org.elasticsearch.http.", "HttpServerTransport");
            bind(HttpServerTransport.class).to(httpServerTransport).asEagerSingleton();
        }

        bind(HttpServer.class).asEagerSingleton();
    }

    public void setHttpServerTransport(Class<? extends HttpServerTransport> httpServerTransport, String source) {
        checkNotNull(httpServerTransport, "Configured http server transport may not be null");
        checkNotNull(source, "Plugin, that changes transport may not be null");
        this.configuredHttpServerTransport = httpServerTransport;
        this.configuredHttpServerTransportSource = source;
    }
}
