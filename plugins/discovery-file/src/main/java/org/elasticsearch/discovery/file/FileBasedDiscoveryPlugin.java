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

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Plugin for providing file-based unicast hosts discovery. The list of unicast hosts
 * is obtained by reading the {@link FileBasedUnicastHostsProvider#UNICAST_HOSTS_FILE} in
 * the {@link Environment#configFile()}/discovery-file directory.
 */
public class FileBasedDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    private final Settings settings;
    private final Path configPath;

    public FileBasedDiscoveryPlugin(Settings settings, Path configPath) {
        this.settings = settings;
        this.configPath = configPath;
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(
            "file",
            () -> new FileBasedUnicastHostsProvider(new Environment(settings, configPath)));
    }
}
