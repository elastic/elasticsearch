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

package org.elasticsearch.client.transport;

import com.google.inject.AbstractModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.DefaultClusterService;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.settings.NoClassSettingsException;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportClientClusterModule extends AbstractModule {

    private final Settings settings;

    public TransportClientClusterModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        try {
            new DiscoveryModule(settings).configure(binder());
            bind(ClusterService.class).to(DefaultClusterService.class).asEagerSingleton();
            bind(TransportClientClusterService.class).asEagerSingleton();
        } catch (NoClassSettingsException e) {
            // that's fine, no actual implementation for discovery
        } catch (Exception e) {
            Loggers.getLogger(getClass(), settings).warn("Failed to load discovery", e);
        }
    }
}
