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

package org.elasticsearch.discovery.file;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class FileBasedDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    private final DeprecationLogger deprecationLogger;
    static final String DEPRECATION_MESSAGE
        = "File-based discovery is now built into Elasticsearch and does not require the discovery-file plugin";

    public FileBasedDiscoveryPlugin() {
        deprecationLogger = new DeprecationLogger(LogManager.getLogger(this.getClass()));
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        deprecationLogger.deprecated(DEPRECATION_MESSAGE);
        return Collections.emptyMap();
    }
}
