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

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.ClassInfo;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.cloud.gce.GceModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.gce.GceUnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.Plugin;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class GceDiscoveryPlugin extends Plugin {

    public static final String GCE = "gce";

    static {
        /*
         * GCE's http client changes access levels because its silly and we
         * can't allow that on any old stack so we pull it here, up front,
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

    @Override
    public Collection<Module> nodeModules() {
        return Collections.singletonList(new GceModule());
    }

    @Override
    @SuppressWarnings("rawtypes") // Supertype uses raw type
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        return Collections.singletonList(GceModule.getComputeServiceImpl());
    }

    public void onModule(DiscoveryModule discoveryModule) {
        discoveryModule.addDiscoveryType(GCE, ZenDiscovery.class);
        // If discovery.type: gce, we add Gce as a unicast provider
            discoveryModule.addUnicastHostProvider(GCE, GceUnicastHostsProvider.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
        // Register GCE settings
        GceComputeService.PROJECT_SETTING,
        GceComputeService.ZONE_SETTING,
        GceUnicastHostsProvider.TAGS_SETTING,
        GceComputeService.REFRESH_SETTING,
        GceComputeService.RETRY_SETTING,
        GceComputeService.MAX_WAIT_SETTING);
    }
}
