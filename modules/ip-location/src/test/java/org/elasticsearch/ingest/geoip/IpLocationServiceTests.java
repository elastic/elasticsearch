/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.DEFAULT_DATABASES;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DatabaseNodeService} acting as an {@link org.elasticsearch.iplocation.api.IpLocationService}.
 * These tests verify the service-level behavior that was formerly tested through
 * GeoIpProcessor.Factory in the unified ingest-geoip module.
 */
@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class IpLocationServiceTests extends ESTestCase {

    /**
     * A class-scoped, read-only directory pre-populated once with the default GeoLite2 mmdbs. The
     * majority of tests in this suite never mutate {@code geoIpConfigDir}; they reuse this single
     * shared directory to avoid re-copying the same databases into a fresh per-test temp dir on every
     * {@code @Before} (and every {@code -Dtests.iters=N} rerun). Tests that need to add files to the
     * config dir or rename files in place opt in to a private mutable copy via
     * {@link #switchToMutableConfigDir()}.
     */
    private static Path sharedReadOnlyConfigDir;

    private Path geoipTmpDir;
    private Path geoIpConfigDir;
    private ConfigDatabases configDatabases;
    private DatabaseNodeService databaseNodeService;
    private ClusterService clusterService;
    private ProjectId projectId;
    private ProjectResolver projectResolver;

    @BeforeClass
    public static void setupSharedReadOnlyConfigDir() throws IOException {
        sharedReadOnlyConfigDir = createTempDir().resolve("ingest-geoip");
        Files.createDirectories(sharedReadOnlyConfigDir);
        copyDefaultDatabases(sharedReadOnlyConfigDir);
    }

    @Before
    public void setup() throws IOException {
        boolean multiProject = randomBoolean();
        projectId = multiProject ? randomProjectIdOrDefault() : ProjectId.DEFAULT;
        projectResolver = multiProject ? TestProjectResolvers.singleProject(projectId) : TestProjectResolvers.DEFAULT_PROJECT_ONLY;

        geoIpConfigDir = sharedReadOnlyConfigDir;
        geoipTmpDir = createTempDir();
        clusterService = mock(ClusterService.class);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        when(clusterService.state()).thenReturn(state);

        GeoIpCache cache = new GeoIpCache(1000);
        configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        for (String name : DEFAULT_DATABASES) {
            configDatabases.updateDatabase(geoIpConfigDir.resolve(name), true);
        }

        databaseNodeService = buildDatabaseNodeService(cache, configDatabases);
    }

    @After
    public void cleanup() throws IOException {
        databaseNodeService.shutdown();
        databaseNodeService = null;
    }

    /**
     * Swaps the shared read-only config dir for a fresh per-test mutable copy and rebuilds the
     * downstream {@link ConfigDatabases} / {@link DatabaseNodeService} stack against it. Use this at
     * the start of any test that mutates {@code geoIpConfigDir} (e.g. via {@code copyDatabase} into
     * the dir, or {@code Files.move}). Keeps test mutations isolated while still letting the bulk of
     * the suite reuse the shared dir from {@link #setupSharedReadOnlyConfigDir()}.
     */
    private void switchToMutableConfigDir() throws IOException {
        databaseNodeService.shutdown();

        geoIpConfigDir = createTempDir().resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);

        GeoIpCache cache = new GeoIpCache(1000);
        configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        copyDefaultDatabases(geoIpConfigDir, configDatabases);

        databaseNodeService = buildDatabaseNodeService(cache, configDatabases);
    }

    private DatabaseNodeService buildDatabaseNodeService(GeoIpCache cache, ConfigDatabases configDatabases) throws IOException {
        DatabaseNodeService service = new DatabaseNodeService(
            geoipTmpDir,
            mock(Client.class),
            cache,
            configDatabases,
            Runnable::run,
            clusterService,
            projectResolver
        );
        service.initialize("nodeId", mock(ResourceWatcherService.class));
        return service;
    }

    public void testCityDefaults() {
        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertThat(lookup, notNullValue());
        assertThat(lookup.getInfo().getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(new HashSet<>(lookup.getInfo().getFields().keySet()), equalTo(fieldNames(Database.City.defaultProperties())));
    }

    public void testCountryDefaults() {
        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-Country.mmdb", null);
        assertThat(lookup, notNullValue());
        assertThat(lookup.getInfo().getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(new HashSet<>(lookup.getInfo().getFields().keySet()), equalTo(fieldNames(Database.Country.defaultProperties())));
    }

    public void testAsnDefaults() {
        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-ASN.mmdb", null);
        assertThat(lookup, notNullValue());
        assertThat(lookup.getInfo().getDatabaseType(), equalTo("GeoLite2-ASN"));
        assertThat(new HashSet<>(lookup.getInfo().getFields().keySet()), equalTo(fieldNames(Database.Asn.defaultProperties())));
    }

    public void testCustomProperties() {
        IpDataLookup lookup = databaseNodeService.createIpDataLookup(
            projectId.id(),
            "GeoLite2-City.mmdb",
            List.of("city_name", "country_name")
        );
        assertThat(lookup, notNullValue());
        assertThat(new HashSet<>(lookup.getInfo().getFields().keySet()), equalTo(Set.of("city_name", "country_name")));
    }

    public void testInvalidProperty() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", List.of("invalid"))
        );
        assertThat(e.getMessage(), containsString("illegal property value [invalid]"));
    }

    public void testCountryDbWithAsnProperty() {
        Set<DatabaseProperty> asnOnlyProperties = new HashSet<>(Database.Asn.properties());
        asnOnlyProperties.remove(DatabaseProperty.IP);
        String asnProperty = randomFrom(asnOnlyProperties).toString();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-Country.mmdb", List.of(asnProperty))
        );
        assertThat(e.getMessage(), containsString("illegal property value [" + asnProperty + "]"));
    }

    public void testAsnDbWithCityProperty() {
        Set<DatabaseProperty> cityOnlyProperties = new HashSet<>(Database.City.properties());
        cityOnlyProperties.remove(DatabaseProperty.IP);
        String cityProperty = randomFrom(cityOnlyProperties).toString();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-ASN.mmdb", List.of(cityProperty))
        );
        assertThat(e.getMessage(), containsString("illegal property value [" + cityProperty + "]"));
    }

    public void testMissingDatabase() {
        cleanDatabases();

        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), randomFrom(DEFAULT_DATABASES), null);
        assertThat(lookup, nullValue());
    }

    public void testNonExistingDbFile() {
        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "does-not-exist.mmdb", null);
        assertThat(lookup, nullValue());
    }

    public void testLazyLoading() throws Exception {
        var configDbs = configDatabases.getConfigDatabases();
        for (DatabaseReaderLazyLoader lazyLoader : configDbs.values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        IpDataLookup cityLookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertThat(cityLookup, notNullValue());

        // these are lazy loaded until first use, so we expect null here
        assertNull(configDbs.get("GeoLite2-City.mmdb").databaseReader.get());
        cityLookup.lookup("1.1.1.1");
        // the first lookup should trigger a database load
        assertNotNull(configDbs.get("GeoLite2-City.mmdb").databaseReader.get());

        IpDataLookup countryLookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-Country.mmdb", null);
        assertThat(countryLookup, notNullValue());

        // these are lazy loaded until first use, so we expect null here
        assertNull(configDbs.get("GeoLite2-Country.mmdb").databaseReader.get());
        countryLookup.lookup("1.1.1.1");
        // the first lookup should trigger a database load
        assertNotNull(configDbs.get("GeoLite2-Country.mmdb").databaseReader.get());

        IpDataLookup asnLookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-ASN.mmdb", null);
        assertThat(asnLookup, notNullValue());

        // these are lazy loaded until first use, so we expect null here
        assertNull(configDbs.get("GeoLite2-ASN.mmdb").databaseReader.get());
        asnLookup.lookup("1.1.1.1");
        // the first lookup should trigger a database load
        assertNotNull(configDbs.get("GeoLite2-ASN.mmdb").databaseReader.get());
    }

    public void testCustomDatabase() throws IOException {
        // mutates the on-disk config dir (Files.move below); switch to a private copy first so other
        // tests that share the read-only dir aren't affected.
        switchToMutableConfigDir();

        // fake the GeoIP2-City database
        copyDatabase("GeoLite2-City.mmdb", geoIpConfigDir);
        Files.move(geoIpConfigDir.resolve("GeoLite2-City.mmdb"), geoIpConfigDir.resolve("GeoIP2-City.mmdb"));
        configDatabases.updateDatabase(geoIpConfigDir.resolve("GeoIP2-City.mmdb"), true);

        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoIP2-City.mmdb", null);
        assertThat(lookup, notNullValue());

        var configDbs = configDatabases.getConfigDatabases();
        // these are lazy loaded until first use, so we expect null here
        assertNull(configDbs.get("GeoIP2-City.mmdb").databaseReader.get());
        lookup.lookup("1.1.1.1");
        // the first lookup should trigger a database load
        assertNotNull(configDbs.get("GeoIP2-City.mmdb").databaseReader.get());
    }

    public void testLookupReflectsDatabaseUpdate() throws Exception {
        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertThat(lookup, notNullValue());

        Map<String, Object> data1 = lookup.lookup("89.160.20.128");
        assertThat(data1, notNullValue());
        assertThat(data1.get("city_name"), equalTo("Tumba"));

        copyDatabase("GeoLite2-City-Test.mmdb", geoipTmpDir);
        databaseNodeService.updateDatabase(projectId, "GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));

        Map<String, Object> data2 = lookup.lookup("89.160.20.128");
        assertThat(data2, notNullValue());
        assertThat(data2.get("city_name"), equalTo("Linköping"));

        // No databases are available, so lookup returns null. Drive the cleanup through the same
        // cluster-state-driven path the service uses in production: a state with an empty task purges
        // every entry from the project's loader map via DatabaseNodeService#checkDatabases.
        PersistentTasksCustomMetadata emptyTasks = DatabaseNodeServiceTests.geoIpDownloaderTask(
            projectId,
            Map.of(),
            projectResolver.supportsMultipleProjects()
        );
        databaseNodeService.checkDatabases(
            DatabaseNodeServiceTests.createClusterState(
                projectId,
                emptyTasks,
                IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST)
            )
        );
        configDatabases.updateDatabase(geoIpConfigDir.resolve("GeoLite2-City.mmdb"), false);

        Map<String, Object> data3 = lookup.lookup("89.160.20.128");
        assertThat(data3, nullValue());

        // There are databases available, but not the right one, so lookup still returns null:
        copyDatabase("GeoLite2-City-Test.mmdb", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));
        databaseNodeService.updateDatabase(projectId, "GeoLite2-City-Test.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));
        Map<String, Object> data4 = lookup.lookup("89.160.20.128");
        assertThat(data4, nullValue());
    }

    public void testDatabaseNotReadyThenAvailable() throws Exception {
        cleanDatabases();

        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertThat(lookup, nullValue());

        copyDatabase("GeoLite2-City-Test.mmdb", geoipTmpDir);
        databaseNodeService.updateDatabase(projectId, "GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));

        lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertThat(lookup, notNullValue());
        Map<String, Object> data = lookup.lookup("89.160.20.128");
        assertThat(data, notNullValue());
        assertThat(data.get("city_name"), equalTo("Linköping"));
    }

    public void testDefaultDatabaseWithTaskPresent() throws Exception {
        PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.builder()
            .addTask(GeoIpDownloader.GEOIP_DOWNLOADER, GeoIpDownloader.GEOIP_DOWNLOADER, null, null)
            .updateTaskState(GeoIpDownloader.GEOIP_DOWNLOADER, GeoIpTaskState.EMPTY)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(PersistentTasksCustomMetadata.TYPE, tasks))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertThat(lookup, notNullValue());

        Map<String, Object> data = lookup.lookup("89.160.20.128");
        assertThat(data, notNullValue());
        assertThat(data.get("city_name"), equalTo("Tumba"));
    }

    public void testLookupReferenceCounting() throws Exception {
        IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertThat(lookup, notNullValue());

        Map<String, Object> data1 = lookup.lookup("8.8.8.8");
        Map<String, Object> data2 = lookup.lookup("89.160.20.128");
        assertThat(data1, notNullValue());
        assertThat(data2, notNullValue());

        try (DatabaseReaderLazyLoader loader = databaseNodeService.getDatabaseReaderLazyLoader(projectId, "GeoLite2-City.mmdb")) {
            assertThat(loader, notNullValue());
            assertThat(loader.current(), equalTo(1));
        }
    }

    public void testGetIpDataLookupInfoConsistentWithCreateIpDataLookup() throws IOException {
        // adds an extra mmdb to the on-disk config dir; switch to a private copy first.
        switchToMutableConfigDir();

        copyDatabase("ipinfo/ip_geolocation_standard_sample.mmdb", geoIpConfigDir.resolve("ip_geolocation_standard_sample.mmdb"));
        configDatabases.updateDatabase(geoIpConfigDir.resolve("ip_geolocation_standard_sample.mmdb"), true);

        List<String> allDatabaseFiles = List.of(
            "GeoLite2-ASN.mmdb",
            "GeoLite2-City.mmdb",
            "GeoLite2-Country.mmdb",
            "ip_geolocation_standard_sample.mmdb"
        );

        for (String databaseFile : allDatabaseFiles) {
            IpDataLookupInfo info = databaseNodeService.getIpDataLookupInfo(databaseFile);
            assertNotNull("getIpDataLookupInfo returned null for [" + databaseFile + "]", info);

            IpDataLookup lookup = databaseNodeService.createIpDataLookup(projectId.id(), databaseFile, null);
            assertNotNull("createIpDataLookup returned null for [" + databaseFile + "]", lookup);

            assertThat(
                "database type mismatch for [" + databaseFile + "]",
                info.getDatabaseType(),
                equalTo(lookup.getInfo().getDatabaseType())
            );

            List<String> allProperties = new ArrayList<>(info.getFields().keySet());
            IpDataLookup fullLookup = databaseNodeService.createIpDataLookup(projectId.id(), databaseFile, allProperties);
            assertNotNull("full-property lookup returned null for [" + databaseFile + "]", fullLookup);

            assertThat(
                "field set mismatch for [" + databaseFile + "]",
                fullLookup.getInfo().getFields().keySet(),
                equalTo(info.getFields().keySet())
            );
        }
    }

    private void cleanDatabases() {
        for (String database : DEFAULT_DATABASES) {
            configDatabases.updateDatabase(geoIpConfigDir.resolve(database), false);
        }
    }

    private static Set<String> fieldNames(Set<DatabaseProperty> properties) {
        Set<String> names = new HashSet<>();
        for (DatabaseProperty prop : properties) {
            names.add(prop.fieldName());
        }
        return names;
    }
}
