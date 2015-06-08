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

package org.elasticsearch.plugin.cloud.azure;

import org.elasticsearch.cloud.azure.AzureModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.azure.AzureRepository;
import org.elasticsearch.repositories.azure.AzureRepositoryModule;

import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.cloud.azure.AzureModule.isSnapshotReady;

/**
 *
 */
public class CloudAzurePlugin extends AbstractPlugin {

    private final Settings settings;
    protected final ESLogger logger = Loggers.getLogger(CloudAzurePlugin.class);

    public CloudAzurePlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting azure plugin...");
    }

    @Override
    public String name() {
        return "cloud-azure";
    }

    @Override
    public String description() {
        return "Cloud Azure Plugin";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        if (AzureModule.isCloudReady(settings)) {
            modules.add(AzureModule.class);
        }
        return modules;
    }

    @Override
    public void processModule(Module module) {
        if (isSnapshotReady(settings, logger)
                && module instanceof RepositoriesModule) {
            ((RepositoriesModule)module).registerRepository(AzureRepository.TYPE, AzureRepositoryModule.class);
        }
    }
}
