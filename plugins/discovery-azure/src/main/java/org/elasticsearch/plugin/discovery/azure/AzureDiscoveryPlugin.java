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

package org.elasticsearch.plugin.discovery.azure;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.azure.AzureDiscoveryModule;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Management;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.azure.AzureDiscovery;
import org.elasticsearch.discovery.azure.AzureUnicastHostsProvider;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AzureDiscoveryPlugin extends Plugin {

    private final Settings settings;
    private final boolean azureDiscoveryActivated;
    protected final ESLogger logger = Loggers.getLogger(AzureDiscoveryPlugin.class);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        azureDiscoveryActivated = AzureDiscovery.AZURE.equals(settings.get("discovery.type"));
        if (azureDiscoveryActivated) {
            logger.trace("starting azure discovery plugin...");
        } else {
            logger.info("You added discovery-azure plugin but not using it. Check your config or remove it.");
        }
    }

    @Override
    public String name() {
        return "discovery-azure";
    }

    @Override
    public String description() {
        return "Azure Discovery Plugin";
    }

    @Override
    public Collection<Module> nodeModules() {
        // The user explicitly asked for a azure discovery. We will fail to start
        // if all settings are not set and correct
        if (azureDiscoveryActivated) {
            // Check that we have all needed properties
            if (isPropertyMissing(settings, Management.SUBSCRIPTION_ID) ||
                isPropertyMissing(settings, Management.SERVICE_NAME) ||
                isPropertyMissing(settings, Management.KEYSTORE_PATH) ||
                isPropertyMissing(settings, Management.KEYSTORE_PASSWORD)
                ) {
                logger.error("one or more azure discovery settings are missing. " +
                        "Check elasticsearch.yml file. Should have [{}], [{}], [{}] and [{}].",
                    Management.SUBSCRIPTION_ID,
                    Management.SERVICE_NAME,
                    Management.KEYSTORE_PATH,
                    Management.KEYSTORE_PASSWORD);
                throw new ElasticsearchException("one or more azure discovery settings are missing. " +
                    "Check elasticsearch.yml file. Should have [{}], [{}], [{}] and [{}].",
                    Management.SUBSCRIPTION_ID,
                    Management.SERVICE_NAME,
                    Management.KEYSTORE_PATH,
                    Management.KEYSTORE_PASSWORD);
            }

            logger.debug("starting azure client service");
            return Collections.singletonList((Module) new AzureDiscoveryModule(settings));
        }
        return Collections.EMPTY_LIST;
    }

    public void onModule(DiscoveryModule discoveryModule) {
        if (azureDiscoveryActivated) {
            discoveryModule.addDiscoveryType("azure", AzureDiscovery.class);
            discoveryModule.addUnicastHostProvider(AzureUnicastHostsProvider.class);
        }
    }

    public static boolean isPropertyMissing(Settings settings, String name) throws ElasticsearchException {
        if (!Strings.hasText(settings.get(name))) {
            return true;
        }
        return false;
    }
}
