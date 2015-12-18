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

package org.elasticsearch.cloud.azure;

import org.elasticsearch.cloud.azure.management.AzureComputeService;
import org.elasticsearch.cloud.azure.management.AzureComputeServiceImpl;
import org.elasticsearch.cloud.azure.management.AzureComputeSettingsFilter;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * Azure Module
 *
 * <ul>
 * <li>If needed this module will bind azure discovery service by default
 * to AzureComputeServiceImpl.</li>
 * </ul>
 *
 * @see org.elasticsearch.cloud.azure.management.AzureComputeServiceImpl
 */
public class AzureDiscoveryModule extends AbstractModule {
    private final ESLogger logger;

    // pkg private so it is settable by tests
    AzureComputeService computeService;

    public AzureDiscoveryModule(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.computeService = new AzureComputeServiceImpl(settings);
    }

    @Override
    protected void configure() {
        logger.debug("starting azure discovery services");
        bind(AzureComputeSettingsFilter.class).asEagerSingleton();
        bind(AzureComputeService.class).toInstance(computeService);

        logger.debug("starting azure client service");
        computeService.configure();
    }
}
