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

import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.cloud.azure.storage.AzureStorageSettingsFilter;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * Azure Module
 *
 * <ul>
 * <li>If needed this module will bind azure repository service by default
 * to AzureStorageServiceImpl.</li>
 * </ul>
 *
 * @see org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl
 */
public class AzureRepositoryModule extends AbstractModule {
    protected final ESLogger logger;

    // pkg private so it is settable by tests
    static Class<? extends AzureStorageService> storageServiceImpl = AzureStorageServiceImpl.class;

    @Inject
    public AzureRepositoryModule(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    protected void configure() {
        logger.debug("starting azure services");
        bind(AzureStorageSettingsFilter.class).asEagerSingleton();

        // If we have settings for azure repository, let's start the azure storage service
        logger.debug("starting azure repository service");
        bind(AzureStorageService.class).to(storageServiceImpl).asEagerSingleton();
    }
}
