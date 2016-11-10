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

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

/**
 * Plugin for providing file-based unicast hosts discovery. The list of unicast hosts
 * is obtained by reading the {@link FileBasedUnicastHostsProvider#UNICAST_HOSTS_FILE} in
 * the {@link Environment#configFile()}/discovery-file directory.
 */
public class FileBasedDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    private static final Logger logger = Loggers.getLogger(FileBasedDiscoveryPlugin.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    private final Settings settings;

    public FileBasedDiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap("file", () -> new FileBasedUnicastHostsProvider(settings, transportService));
    }

    @Override
    public Settings additionalSettings() {
        // For 5.0, the hosts provider was "zen", but this was before the discovery.zen.hosts_provider
        // setting existed. This check looks for the legacy zen, and sets the file hosts provider if not set
        String discoveryType = DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings);
        if (discoveryType.equals("zen")) {
            deprecationLogger.deprecated("Using " + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() +
                " setting to set hosts provider is deprecated. " +
                "Set \"" + DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey() + ": file\" instead");
            if (DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.exists(settings) == false) {
                return Settings.builder().put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "file").build();
            }
        }
        return Settings.EMPTY;
    }
}
