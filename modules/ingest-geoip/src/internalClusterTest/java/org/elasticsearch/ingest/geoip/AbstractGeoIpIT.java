/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.StreamsUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public abstract class AbstractGeoIpIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal, final Settings otherSettings) {
        final Path databasePath = createTempDir();
        try {
            Files.createDirectories(databasePath);
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")),
                databasePath.resolve("GeoLite2-City.mmdb")
            );
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb")),
                databasePath.resolve("GeoLite2-Country.mmdb")
            );
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-ASN.mmdb")),
                databasePath.resolve("GeoLite2-ASN.mmdb")
            );
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return Settings.builder()
            .put("ingest.geoip.database_path", databasePath)
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
