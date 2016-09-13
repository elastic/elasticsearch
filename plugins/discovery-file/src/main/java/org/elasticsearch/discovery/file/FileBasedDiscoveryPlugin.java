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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;

/**
 * Plugin for providing file-based unicast hosts discovery. The list of unicast hosts
 * is obtained by reading the {@link FileBasedUnicastHostsProvider#UNICAST_HOSTS_FILE} in
 * the {@link Environment#configFile()}/discovery-file directory.
 */
public class FileBasedDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    private static final Logger logger = Loggers.getLogger(FileBasedDiscoveryPlugin.class);

    private final Settings settings;

    public FileBasedDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting file-based discovery plugin...");
    }

    public void onModule(DiscoveryModule discoveryModule) {
        logger.trace("registering file-based unicast hosts provider");
        // using zen discovery for the discovery type and we're just adding a unicast host provider for it
        discoveryModule.addUnicastHostProvider("zen", FileBasedUnicastHostsProvider.class);
    }
}
