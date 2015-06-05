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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.cloud.azure.AzureModule;
import org.elasticsearch.cloud.azure.management.AzureComputeSettingsFilter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.ZenDiscoveryModule;

/**
 *
 */
public class AzureDiscoveryModule extends ZenDiscoveryModule {

    protected final ESLogger logger;
    private Settings settings;

    public AzureDiscoveryModule(Settings settings) {
        super();
        this.logger = Loggers.getLogger(getClass(), settings);
        this.settings = settings;
        if (AzureModule.isDiscoveryReady(settings, logger)) {
            addUnicastHostProvider(AzureUnicastHostsProvider.class);
        }
    }

    @Override
    protected void bindDiscovery() {
        bind(AzureComputeSettingsFilter.class).asEagerSingleton();
        if (AzureModule.isDiscoveryReady(settings, logger)) {
            bind(Discovery.class).to(AzureDiscovery.class).asEagerSingleton();
        } else {
            logger.debug("disabling azure discovery features");
        }
    }
}
