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

package org.elasticsearch.plugin.discovery.gce;

import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.cloud.gce.GceModule;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.gce.GceDiscovery;
import org.elasticsearch.discovery.gce.GceUnicastHostsProvider;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class GceDiscoveryPlugin extends Plugin {

    private final Settings settings;
    protected final ESLogger logger = Loggers.getLogger(GceDiscoveryPlugin.class);

    public GceDiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "discovery-gce";
    }

    @Override
    public String description() {
        return "Cloud Google Compute Engine Discovery Plugin";
    }

    @Override
    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        if (isDiscoveryAlive(settings, logger)) {
            modules.add(new GceModule());
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (isDiscoveryAlive(settings, logger)) {
            services.add(GceModule.getComputeServiceImpl());
        }
        return services;
    }

    public void onModule(DiscoveryModule discoveryModule) {
        if (isDiscoveryAlive(settings, logger)) {
            discoveryModule.addDiscoveryType("gce", GceDiscovery.class);
            discoveryModule.addUnicastHostProvider(GceUnicastHostsProvider.class);
        }
    }

    /**
     * Check if discovery is meant to start
     *
     * @return true if we can start gce discovery features
     */
    public static boolean isDiscoveryAlive(Settings settings, ESLogger logger) {
        // User set discovery.type: gce
        if (GceDiscovery.GCE.equalsIgnoreCase(settings.get("discovery.type")) == false) {
            logger.debug("discovery.type not set to {}", GceDiscovery.GCE);
            return false;
        }

        if (checkProperty(GceComputeService.Fields.PROJECT, settings.get(GceComputeService.Fields.PROJECT), logger) == false ||
                checkProperty(GceComputeService.Fields.ZONE, settings.getAsArray(GceComputeService.Fields.ZONE), logger) == false) {
            logger.debug("one or more gce discovery settings are missing. " +
                            "Check elasticsearch.yml file. Should have [{}] and [{}].",
                    GceComputeService.Fields.PROJECT,
                    GceComputeService.Fields.ZONE);
            return false;
        }

        logger.trace("all required properties for gce discovery are set!");

        return true;
    }

    private static boolean checkProperty(String name, String value, ESLogger logger) {
        if (!Strings.hasText(value)) {
            logger.warn("{} is not set.", name);
            return false;
        }
        return true;
    }

    private static boolean checkProperty(String name, String[] values, ESLogger logger) {
        if (values == null || values.length == 0) {
            logger.warn("{} is not set.", name);
            return false;
        }
        return true;
    }

}
