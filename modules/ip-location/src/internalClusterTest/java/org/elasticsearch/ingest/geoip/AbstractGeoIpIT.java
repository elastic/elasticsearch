/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import fixture.geoip.GeoIpHttpFixture;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;

public abstract class AbstractGeoIpIT extends ESIntegTestCase {
    private static final boolean useFixture = Booleans.parseBoolean(System.getProperty("geoip_use_service", "false")) == false;

    @ClassRule
    public static final GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    /**
     * Shared across every node and every test method in a single IT class. Populated once in
     * {@link #setupSharedDatabasePath()} so {@code nodeSettings(...)} doesn't allocate a fresh temp dir
     * and re-copy every default mmdb for each node × method × {@code -Dtests.iters=N} combination.
     * No IT in this hierarchy mutates this directory; the {@code ingest.geoip.database_path} contract
     * already permits multiple nodes to share one dir on a single host.
     */
    private static Path sharedDatabasePath;

    @BeforeClass
    public static void setupSharedDatabasePath() {
        sharedDatabasePath = createTempDir();
        copyDefaultDatabases(sharedDatabasePath);
    }

    protected String getEndpoint() {
        return useFixture ? fixture.getAddress() : null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal, final Settings otherSettings) {
        return Settings.builder()
            .put("ingest.geoip.database_path", sharedDatabasePath)
            .put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false)
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .build();
    }

    public static class IngestGeoIpSettingsPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.simpleString("ingest.geoip.database_path", Setting.Property.NodeScope),
                Setting.timeSetting(
                    "ingest.geoip.database_validity",
                    TimeValue.timeValueDays(3),
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic
                )
            );
        }
    }
}
