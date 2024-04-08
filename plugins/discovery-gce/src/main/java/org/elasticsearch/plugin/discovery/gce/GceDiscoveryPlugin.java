/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.discovery.gce;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.ClassInfo;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cloud.gce.GceInstancesService;
import org.elasticsearch.cloud.gce.GceInstancesServiceImpl;
import org.elasticsearch.cloud.gce.GceMetadataService;
import org.elasticsearch.cloud.gce.network.GceNameResolver;
import org.elasticsearch.cloud.gce.util.Access;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.gce.GceSeedHostsProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GceDiscoveryPlugin extends Plugin implements DiscoveryPlugin, Closeable {

    /** Determines whether settings those reroutes GCE call should be allowed (for testing purposes only). */
    private static final boolean ALLOW_REROUTE_GCE_SETTINGS = Booleans.parseBoolean(
        System.getProperty("es.allow_reroute_gce_settings", "false")
    );

    public static final String GCE = "gce";
    protected final Settings settings;
    private static final Logger logger = LogManager.getLogger(GceDiscoveryPlugin.class);
    // stashed when created in order to properly close
    private final SetOnce<GceInstancesService> gceInstancesService = new SetOnce<>();

    static {
        /*
         * GCE's http client changes access levels because its silly and we
         * can't allow that on any old stack so we pull it here, up front,
         * so we can cleanly check the permissions for it. Without this changing
         * the permission can fail if any part of core is on the stack because
         * our plugin permissions don't allow core to "reach through" plugins to
         * change the permission. Because that'd be silly.
         */
        Access.doPrivilegedVoid(() -> ClassInfo.of(HttpHeaders.class, true));
    }

    public GceDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting gce discovery plugin...");
    }

    // overrideable for tests
    protected GceInstancesService createGceInstancesService() {
        return new GceInstancesServiceImpl(settings);
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        return Collections.singletonMap(GCE, () -> {
            gceInstancesService.set(createGceInstancesService());
            return new GceSeedHostsProvider(settings, gceInstancesService.get(), transportService, networkService);
        });
    }

    @Override
    public NetworkService.CustomNameResolver getCustomNameResolver(Settings settingsToUse) {
        logger.debug("Register _gce_, _gce:xxx network names");
        return new GceNameResolver(new GceMetadataService(settingsToUse));
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>(
            Arrays.asList(
                // Register GCE settings
                GceInstancesService.PROJECT_SETTING,
                GceInstancesService.ZONE_SETTING,
                GceSeedHostsProvider.TAGS_SETTING,
                GceInstancesService.REFRESH_SETTING,
                GceInstancesService.RETRY_SETTING,
                GceInstancesService.MAX_WAIT_SETTING
            )
        );

        if (ALLOW_REROUTE_GCE_SETTINGS) {
            settingList.add(GceMetadataService.GCE_HOST);
            settingList.add(GceInstancesServiceImpl.GCE_ROOT_URL);
        }
        return Collections.unmodifiableList(settingList);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(gceInstancesService.get());
    }
}
