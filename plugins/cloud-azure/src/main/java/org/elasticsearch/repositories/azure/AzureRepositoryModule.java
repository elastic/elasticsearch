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

package org.elasticsearch.repositories.azure;

import org.elasticsearch.cloud.azure.AzureModule;
import org.elasticsearch.cloud.azure.storage.AzureStorageSettingsFilter;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.repositories.Repository;

/**
 * Azure repository module
 */
public class AzureRepositoryModule extends AbstractModule {

    protected final ESLogger logger;
    private Settings settings;

    public AzureRepositoryModule(Settings settings) {
        super();
        this.logger = Loggers.getLogger(getClass(), settings);
        this.settings = settings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        bind(AzureStorageSettingsFilter.class).asEagerSingleton();
        if (AzureModule.isSnapshotReady(settings, logger)) {
            bind(Repository.class).to(AzureRepository.class).asEagerSingleton();
            bind(IndexShardRepository.class).to(BlobStoreIndexShardRepository.class).asEagerSingleton();
        } else {
            logger.debug("disabling azure snapshot and restore features");
        }
    }

}

