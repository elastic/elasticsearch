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

package org.elasticsearch.plugin.cloud.gce;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.ClassInfo;
import org.elasticsearch.SpecialPermission;
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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class CloudGcePlugin extends Plugin {
    static {
        /*
         * GCE's http client changes access levels because its silly and we
         * can't allow that on any old stack stack so we pull it here, up front,
         * so we can cleanly check the permissions for it. Without this changing
         * the permission can fail if any part of core is on the stack because
         * our plugin permissions don't allow core to "reach through" plugins to
         * change the permission. Because that'd be silly.
         */
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                ClassInfo.of(HttpHeaders.class, true);
                return null;
            }
        });
    }

    private final Settings settings;
    protected final ESLogger logger = Loggers.getLogger(CloudGcePlugin.class);

    public CloudGcePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "cloud-gce";
    }

    @Override
    public String description() {
        return "Cloud Google Compute Engine Plugin";
    }

    @Override
    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(new GceModule(settings));
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (isDiscoveryAlive(settings, logger)) {
            services.add(GceModule.getComputeServiceImpl());
        }
        services.add(GceModule.getMetadataServiceImpl());
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

        if (checkProperty(settings.get(GceComputeService.Fields.PROJECT)) == false ||
                checkProperty(settings.getAsArray(GceComputeService.Fields.ZONE)) == false) {
            logger.warn("one or more gce discovery settings are missing. " +
                            "Check elasticsearch.yml file. Should have [{}] and [{}].",
                    GceComputeService.Fields.PROJECT,
                    GceComputeService.Fields.ZONE);
            return false;
        }

        logger.trace("all required properties for gce discovery are set!");

        return true;
    }

    private static boolean checkProperty(String value) {
        return Strings.hasText(value);
    }

    private static boolean checkProperty(String[] values) {
        return !(values == null || values.length == 0);
    }

}
