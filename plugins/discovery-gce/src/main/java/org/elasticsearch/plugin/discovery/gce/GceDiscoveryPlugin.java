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
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cloud.gce.GceInstancesService;
import org.elasticsearch.cloud.gce.GceInstancesServiceImpl;
import org.elasticsearch.cloud.gce.GceMetadataService;
import org.elasticsearch.cloud.gce.network.GceNameResolver;
import org.elasticsearch.cloud.gce.util.Access;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.gce.GceUnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GceDiscoveryPlugin extends Plugin implements DiscoveryPlugin, Closeable {

    public static final String GCE = "gce";
    private final Settings settings;
    private static final Logger logger = Loggers.getLogger(GceDiscoveryPlugin.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    // stashed when created in order to properly close
    private final SetOnce<GceInstancesServiceImpl> gceInstancesService = new SetOnce<>();

    static {
        /*
         * GCE's http client changes access levels because its silly and we
         * can't allow that on any old stack so we pull it here, up front,
         * so we can cleanly check the permissions for it. Without this changing
         * the permission can fail if any part of core is on the stack because
         * our plugin permissions don't allow core to "reach through" plugins to
         * change the permission. Because that'd be silly.
         */
        Access.doPrivilegedVoid( () -> ClassInfo.of(HttpHeaders.class, true));
    }

    public GceDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting gce discovery plugin...");
    }



    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(GCE, () -> {
            gceInstancesService.set(new GceInstancesServiceImpl(settings));
            return new GceUnicastHostsProvider(settings, gceInstancesService.get(), transportService, networkService);
        });
    }

    @Override
    public NetworkService.CustomNameResolver getCustomNameResolver(Settings settings) {
        logger.debug("Register _gce_, _gce:xxx network names");
        return new GceNameResolver(settings, new GceMetadataService(settings));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // Register GCE settings
            GceInstancesService.PROJECT_SETTING,
            GceInstancesService.ZONE_SETTING,
            GceUnicastHostsProvider.TAGS_SETTING,
            GceInstancesService.REFRESH_SETTING,
            GceInstancesService.RETRY_SETTING,
            GceInstancesService.MAX_WAIT_SETTING);
    }



    @Override
    public void close() throws IOException {
        IOUtils.close(gceInstancesService.get());
    }
}
