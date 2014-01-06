/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.cloud.azure;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.azure.AzureDiscovery;

/**
 * Azure Module
 *
 * <ul>
 * <li>If needed this module will bind azure discovery service by default
 * to AzureComputeServiceImpl.</li>
 * </ul>
 *
 * @see org.elasticsearch.cloud.azure.AzureComputeServiceImpl
 */
public class AzureModule extends AbstractModule {
    protected final ESLogger logger;
    private Settings settings;

    @Inject
    public AzureModule(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    protected void configure() {
        logger.debug("starting azure services");

        // If we have set discovery to azure, let's start the azure compute service
        if (isDiscoveryReady(settings)) {
            logger.debug("starting azure discovery service");
        bind(AzureComputeService.class)
                .to(settings.getAsClass("cloud.azure.api.impl", AzureComputeServiceImpl.class))
                .asEagerSingleton();
        }
    }

    /**
     * Check if discovery is meant to start
     * @return true if we can start discovery features
     */
    public static boolean isDiscoveryReady(Settings settings) {
        return (AzureDiscovery.AZURE.equalsIgnoreCase(settings.get("discovery.type")));
    }
}
