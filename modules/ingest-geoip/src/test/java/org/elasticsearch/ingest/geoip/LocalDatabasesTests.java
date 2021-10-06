/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.model.CityResponse;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.hamcrest.Matchers.equalTo;

public class LocalDatabasesTests extends ESTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(LocalDatabases.class.getSimpleName());
        Settings settings = Settings.builder().put("resource.reload.interval.high", TimeValue.timeValueMillis(100)).build();
        resourceWatcherService = new ResourceWatcherService(settings, threadPool);
    }

    @After
    public void cleanup() {
        resourceWatcherService.close();
        threadPool.shutdownNow();
    }

    public void testLocalDatabasesEmptyConfig() throws Exception {
        Path configDir = createTempDir();
        LocalDatabases localDatabases = new LocalDatabases(prepareModuleDir(), configDir, new GeoIpCache(0));
        localDatabases.initialize(resourceWatcherService);

        assertThat(localDatabases.getDefaultDatabases().size(), equalTo(3));
        assertThat(localDatabases.getConfigDatabases().size(), equalTo(0));
        DatabaseReaderLazyLoader loader = localDatabases.getDatabase("GeoLite2-ASN.mmdb", true);
        assertThat(loader.getDatabaseType(), equalTo("GeoLite2-ASN"));

        loader = localDatabases.getDatabase("GeoLite2-City.mmdb", true);
        assertThat(loader.getDatabaseType(), equalTo("GeoLite2-City"));

        loader = localDatabases.getDatabase("GeoLite2-Country.mmdb", true);
        assertThat(loader.getDatabaseType(), equalTo("GeoLite2-Country"));
    }

    public void testDatabasesConfigDir() throws Exception {
        Path configDir = createTempDir();
        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoIP2-City-Test.mmdb"), configDir.resolve("GeoIP2-City.mmdb"));
        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoLite2-City-Test.mmdb"), configDir.resolve("GeoLite2-City.mmdb"));

        LocalDatabases localDatabases = new LocalDatabases(prepareModuleDir(), configDir, new GeoIpCache(0));
        localDatabases.initialize(resourceWatcherService);

        assertThat(localDatabases.getDefaultDatabases().size(), equalTo(3));
        assertThat(localDatabases.getConfigDatabases().size(), equalTo(2));
        DatabaseReaderLazyLoader loader = localDatabases.getDatabase("GeoLite2-ASN.mmdb", true);
        assertThat(loader.getDatabaseType(), equalTo("GeoLite2-ASN"));

        loader = localDatabases.getDatabase("GeoLite2-City.mmdb", true);
        assertThat(loader.getDatabaseType(), equalTo("GeoLite2-City"));

        loader = localDatabases.getDatabase("GeoLite2-Country.mmdb", true);
        assertThat(loader.getDatabaseType(), equalTo("GeoLite2-Country"));

        loader = localDatabases.getDatabase("GeoIP2-City.mmdb", true);
        assertThat(loader.getDatabaseType(), equalTo("GeoIP2-City"));
    }

    public void testDatabasesDynamicUpdateConfigDir() throws Exception {
        Path configDir = createTempDir();
        LocalDatabases localDatabases = new LocalDatabases(prepareModuleDir(), configDir, new GeoIpCache(0));
        localDatabases.initialize(resourceWatcherService);
        {
            assertThat(localDatabases.getDefaultDatabases().size(), equalTo(3));
            DatabaseReaderLazyLoader loader = localDatabases.getDatabase("GeoLite2-ASN.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-ASN"));

            loader = localDatabases.getDatabase("GeoLite2-City.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-City"));

            loader = localDatabases.getDatabase("GeoLite2-Country.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-Country"));
        }

        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoIP2-City-Test.mmdb"), configDir.resolve("GeoIP2-City.mmdb"));
        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoLite2-City-Test.mmdb"), configDir.resolve("GeoLite2-City.mmdb"));
        assertBusy(() -> {
            assertThat(localDatabases.getDefaultDatabases().size(), equalTo(3));
            assertThat(localDatabases.getConfigDatabases().size(), equalTo(2));
            DatabaseReaderLazyLoader loader = localDatabases.getDatabase("GeoLite2-ASN.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-ASN"));

            loader = localDatabases.getDatabase("GeoLite2-City.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-City"));

            loader = localDatabases.getDatabase("GeoLite2-Country.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-Country"));

            loader = localDatabases.getDatabase("GeoIP2-City.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoIP2-City"));
        });
    }

    public void testDatabasesUpdateExistingConfDatabase() throws Exception {
        Path configDir = createTempDir();
        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoLite2-City.mmdb"), configDir.resolve("GeoLite2-City.mmdb"));
        GeoIpCache cache = new GeoIpCache(1000); // real cache to test purging of entries upon a reload
        LocalDatabases localDatabases = new LocalDatabases(prepareModuleDir(), configDir, cache);
        localDatabases.initialize(resourceWatcherService);
        {
            assertThat(cache.count(), equalTo(0));
            assertThat(localDatabases.getDefaultDatabases().size(), equalTo(3));
            assertThat(localDatabases.getConfigDatabases().size(), equalTo(1));

            DatabaseReaderLazyLoader loader = localDatabases.getDatabase("GeoLite2-City.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-City"));
            CityResponse cityResponse = loader.getCity(InetAddresses.forString("89.160.20.128"));
            assertThat(cityResponse.getCity().getName(), equalTo("Tumba"));
            assertThat(cache.count(), equalTo(1));
        }

        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoLite2-City-Test.mmdb"), configDir.resolve("GeoLite2-City.mmdb"),
            StandardCopyOption.REPLACE_EXISTING);
        assertBusy(() -> {
            assertThat(localDatabases.getDefaultDatabases().size(), equalTo(3));
            assertThat(localDatabases.getConfigDatabases().size(), equalTo(1));
            assertThat(cache.count(), equalTo(0));

            DatabaseReaderLazyLoader loader = localDatabases.getDatabase("GeoLite2-City.mmdb", true);
            assertThat(loader.getDatabaseType(), equalTo("GeoLite2-City"));
            CityResponse cityResponse = loader.getCity(InetAddresses.forString("89.160.20.128"));
            assertThat(cityResponse.getCity().getName(), equalTo("Linköping"));
            assertThat(cache.count(), equalTo(1));
        });

        Files.delete(configDir.resolve("GeoLite2-City.mmdb"));
        assertBusy(() -> {
            assertThat(localDatabases.getDefaultDatabases().size(), equalTo(3));
            assertThat(localDatabases.getConfigDatabases().size(), equalTo(0));
            assertThat(cache.count(), equalTo(0));
        });
    }

    private static Path prepareModuleDir() throws IOException {
        Path dir = createTempDir();
        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoLite2-ASN.mmdb"), dir.resolve("GeoLite2-ASN.mmdb"));
        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoLite2-City.mmdb"), dir.resolve("GeoLite2-City.mmdb"));
        Files.copy(LocalDatabases.class.getResourceAsStream("/GeoLite2-Country.mmdb"), dir.resolve("GeoLite2-Country.mmdb"));
        return dir;
    }

}
