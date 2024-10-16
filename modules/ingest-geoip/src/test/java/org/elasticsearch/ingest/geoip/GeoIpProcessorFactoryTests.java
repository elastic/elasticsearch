/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.geoip.Database.Property;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.geoip.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.DEFAULT_DATABASES;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpProcessorFactoryTests extends ESTestCase {

    private Path geoipTmpDir;
    private Path geoIpConfigDir;
    private ConfigDatabases configDatabases;
    private DatabaseNodeService databaseNodeService;
    private ClusterService clusterService;

    @Before
    public void loadDatabaseReaders() throws IOException {
        final Path configDir = createTempDir();
        geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);

        Client client = mock(Client.class);
        GeoIpCache cache = new GeoIpCache(1000);
        configDatabases = new ConfigDatabases(geoIpConfigDir, new GeoIpCache(1000));
        copyDefaultDatabases(geoIpConfigDir, configDatabases);
        geoipTmpDir = createTempDir();
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        databaseNodeService = new DatabaseNodeService(geoipTmpDir, client, cache, configDatabases, Runnable::run, clusterService);
        databaseNodeService.initialize("nodeId", mock(ResourceWatcherService.class), mock(IngestService.class));
    }

    @After
    public void closeDatabaseReaders() throws IOException {
        databaseNodeService.shutdown();
        databaseNodeService = null;
    }

    public void testBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(Database.City.defaultProperties()));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(Database.City.defaultProperties()));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCountryBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(Database.Country.defaultProperties()));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testAsnBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-ASN"));
        assertThat(processor.getProperties(), sameInstance(Database.Asn.defaultProperties()));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(Database.Country.defaultProperties()));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildWithCountryDbAndAsnFields() {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        Set<Property> asnOnlyProperties = new HashSet<>(Database.Asn.properties());
        asnOnlyProperties.remove(Property.IP);
        String asnProperty = RandomPicks.randomFrom(Randomness.get(), asnOnlyProperties).toString();
        config.put("properties", List.of(asnProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(
            e.getMessage(),
            equalTo(
                "[properties] illegal property value ["
                    + asnProperty
                    + "]. valid values are [IP, COUNTRY_IN_EUROPEAN_UNION, COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_CODE, "
                    + "CONTINENT_NAME, REGISTERED_COUNTRY_IN_EUROPEAN_UNION, REGISTERED_COUNTRY_ISO_CODE, REGISTERED_COUNTRY_NAME]"
            )
        );
    }

    public void testBuildWithAsnDbAndCityFields() {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        Set<Property> cityOnlyProperties = new HashSet<>(Database.City.properties());
        cityOnlyProperties.remove(Property.IP);
        String cityProperty = RandomPicks.randomFrom(Randomness.get(), cityOnlyProperties).toString();
        config.put("properties", List.of(cityProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(
            e.getMessage(),
            equalTo("[properties] illegal property value [" + cityProperty + "]. valid values are [IP, ASN, ORGANIZATION_NAME, NETWORK]")
        );
    }

    public void testBuildNonExistingDbFile() throws Exception {
        copyDatabase("GeoLite2-City-Test.mmdb", geoipTmpDir.resolve("GeoLite2-City.mmdb"));
        databaseNodeService.updateDatabase("GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City.mmdb"));
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "does-not-exist.mmdb");
        Processor processor = factory.create(null, null, null, config);
        assertThat(processor, instanceOf(GeoIpProcessor.DatabaseUnavailableProcessor.class));
    }

    public void testBuildBuiltinDatabaseMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        cleanDatabases(geoIpConfigDir, configDatabases);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", randomFrom(DEFAULT_DATABASES));
        Processor processor = factory.create(null, null, null, config);
        assertThat(processor, instanceOf(GeoIpProcessor.DatabaseUnavailableProcessor.class));
    }

    public void testBuildFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Set<Property> properties = new HashSet<>();
        List<String> fieldNames = new ArrayList<>();

        int counter = 0;
        int numFields = scaledRandomIntBetween(1, Property.values().length);
        for (Property property : Database.City.properties()) {
            properties.add(property);
            fieldNames.add(property.name().toLowerCase(Locale.ROOT));
            if (++counter >= numFields) {
                break;
            }
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(properties));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildIllegalFieldOption() {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("field", "_field");
        config1.put("properties", List.of("invalid"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1));
        assertThat(
            e.getMessage(),
            equalTo(
                "[properties] illegal property value [invalid]. valid values are [IP, COUNTRY_IN_EUROPEAN_UNION, COUNTRY_ISO_CODE, "
                    + "COUNTRY_NAME, CONTINENT_CODE, CONTINENT_NAME, REGION_ISO_CODE, REGION_NAME, CITY_NAME, TIMEZONE, "
                    + "LOCATION, POSTAL_CODE, ACCURACY_RADIUS, REGISTERED_COUNTRY_IN_EUROPEAN_UNION, REGISTERED_COUNTRY_ISO_CODE, "
                    + "REGISTERED_COUNTRY_NAME]"
            )
        );

        Map<String, Object> config2 = new HashMap<>();
        config2.put("field", "_field");
        config2.put("properties", "invalid");
        e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config2));
        assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
    }

    public void testBuildUnsupportedDatabase() throws Exception {
        // mock up some unsupported database (it has a databaseType that we don't recognize)
        IpDatabase database = mock(IpDatabase.class);
        when(database.getDatabaseType()).thenReturn("some-unsupported-database");
        IpDatabaseProvider provider = mock(IpDatabaseProvider.class);
        when(provider.getDatabase(anyString())).thenReturn(database);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, provider);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("field", "_field");
        config1.put("properties", List.of("ip"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1));
        assertThat(
            e.getMessage(),
            equalTo("[database_file] Unsupported database type [some-unsupported-database] for file [GeoLite2-City.mmdb]")
        );
    }

    public void testBuildNullDatabase() throws Exception {
        // mock up a provider that returns a null databaseType
        IpDatabase database = mock(IpDatabase.class);
        when(database.getDatabaseType()).thenReturn(null);
        IpDatabaseProvider provider = mock(IpDatabaseProvider.class);
        when(provider.getDatabase(anyString())).thenReturn(database);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, provider);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("field", "_field");
        config1.put("properties", List.of("ip"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1));
        assertThat(e.getMessage(), equalTo("[database_file] Unsupported database type [null] for file [GeoLite2-City.mmdb]"));
    }

    public void testStrictMaxmindSupport() throws Exception {
        IpDatabase database = mock(IpDatabase.class);
        when(database.getDatabaseType()).thenReturn("ipinfo some_ipinfo_database.mmdb-City");
        IpDatabaseProvider provider = mock(IpDatabaseProvider.class);
        when(provider.getDatabase(anyString())).thenReturn(database);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, provider);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("database_file", "some-ipinfo-database.mmdb");
        config1.put("field", "_field");
        config1.put("properties", List.of("ip"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1));
        assertThat(
            e.getMessage(),
            equalTo(
                "[database_file] Unsupported database type [ipinfo some_ipinfo_database.mmdb-City] "
                    + "for file [some-ipinfo-database.mmdb]"
            )
        );
    }

    public void testLaxMaxmindSupport() throws Exception {
        IpDatabase database = mock(IpDatabase.class);
        when(database.getDatabaseType()).thenReturn("some_custom_database.mmdb-City");
        IpDatabaseProvider provider = mock(IpDatabaseProvider.class);
        when(provider.getDatabase(anyString())).thenReturn(database);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, provider);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("database_file", "some-custom-database.mmdb");
        config1.put("field", "_field");
        config1.put("properties", List.of("ip"));
        factory.create(null, null, null, config1);
        assertWarnings(GeoIpProcessor.UNSUPPORTED_DATABASE_DEPRECATION_MESSAGE.replaceAll("\\{}", "some_custom_database.mmdb-City"));
    }

    public void testLazyLoading() throws Exception {
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        GeoIpCache cache = new GeoIpCache(1000);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        copyDefaultDatabases(geoIpConfigDir, configDatabases);

        // Loading another database reader instances, because otherwise we can't test lazy loading as the
        // database readers used at class level are reused between tests. (we want to keep that otherwise running this
        // test will take roughly 4 times more time)
        Client client = mock(Client.class);
        DatabaseNodeService databaseNodeService = new DatabaseNodeService(
            createTempDir(),
            client,
            cache,
            configDatabases,
            Runnable::run,
            clusterService
        );
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        for (DatabaseReaderLazyLoader lazyLoader : configDatabases.getConfigDatabases().values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Map.of("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", 1L, "routing", VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-City.mmdb");
        final GeoIpProcessor city = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use, so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-City.mmdb").databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-City.mmdb").databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        final GeoIpProcessor country = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use, so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-Country.mmdb").databaseReader.get());
        country.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-Country.mmdb").databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        final GeoIpProcessor asn = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use, so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-ASN.mmdb").databaseReader.get());
        asn.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-ASN.mmdb").databaseReader.get());
    }

    public void testLoadingCustomDatabase() throws IOException {
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, new GeoIpCache(1000));
        copyDefaultDatabases(geoIpConfigDir, configDatabases);
        // fake the GeoIP2-City database
        copyDatabase("GeoLite2-City.mmdb", geoIpConfigDir);
        Files.move(geoIpConfigDir.resolve("GeoLite2-City.mmdb"), geoIpConfigDir.resolve("GeoIP2-City.mmdb"));

        /*
         * Loading another database reader instances, because otherwise we can't test lazy loading as the database readers used at class
         * level are reused between tests. (we want to keep that otherwise running this test will take roughly 4 times more time).
         */
        ThreadPool threadPool = new TestThreadPool("test");
        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        Client client = mock(Client.class);
        GeoIpCache cache = new GeoIpCache(1000);
        DatabaseNodeService databaseNodeService = new DatabaseNodeService(
            createTempDir(),
            client,
            cache,
            configDatabases,
            Runnable::run,
            clusterService
        );
        databaseNodeService.initialize("nodeId", resourceWatcherService, mock(IngestService.class));
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        for (DatabaseReaderLazyLoader lazyLoader : configDatabases.getConfigDatabases().values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Map.of("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", 1L, "routing", VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoIP2-City.mmdb");
        final GeoIpProcessor city = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use, so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoIP2-City.mmdb").databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoIP2-City.mmdb").databaseReader.get());
        resourceWatcherService.close();
        threadPool.shutdown();
    }

    public void testFallbackUsingDefaultDatabases() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("fallback_to_default_databases", randomBoolean());
        factory.create(null, null, null, config);
        assertWarnings(GeoIpProcessor.DEFAULT_DATABASES_DEPRECATION_MESSAGE);
    }

    public void testDownloadDatabaseOnPipelineCreation() throws IOException {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomIdentifier());
        config.put("download_database_on_pipeline_creation", randomBoolean());
        factory.create(null, null, null, config);
        // Check all the config params were consumed.
        assertThat(config, anEmptyMap());
    }

    public void testDefaultDatabaseWithTaskPresent() throws Exception {
        PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.builder()
            .addTask(GeoIpDownloader.GEOIP_DOWNLOADER, GeoIpDownloader.GEOIP_DOWNLOADER, null, null)
            .updateTaskState(GeoIpDownloader.GEOIP_DOWNLOADER, GeoIpTaskState.EMPTY)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);

        processor.execute(RandomDocumentPicks.randomIngestDocument(random(), Map.of("_field", "89.160.20.128")));
    }

    public void testUpdateDatabaseWhileIngesting() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config);
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "89.160.20.128");
        {
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData.get("city_name"), equalTo("Tumba"));
        }
        {
            copyDatabase("GeoLite2-City-Test.mmdb", geoipTmpDir);
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            databaseNodeService.updateDatabase("GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));
            processor.execute(ingestDocument);
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData.get("city_name"), equalTo("Linköping"));
        }
        {
            // No databases are available, so assume that databases still need to be downloaded and therefore not fail:
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            databaseNodeService.removeStaleEntries(List.of("GeoLite2-City.mmdb"));
            configDatabases.updateDatabase(geoIpConfigDir.resolve("GeoLite2-City.mmdb"), false);
            processor.execute(ingestDocument);
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData, nullValue());
        }
        {
            // There are databases available, but not the right one, so tag:
            databaseNodeService.updateDatabase("GeoLite2-City-Test.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getSourceAndMetadata(), hasEntry("tags", List.of("_geoip_database_unavailable_GeoLite2-City.mmdb")));
        }
    }

    public void testDatabaseNotReadyYet() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        cleanDatabases(geoIpConfigDir, configDatabases);

        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "source_field");
            config.put("database_file", "GeoLite2-City.mmdb");

            Map<String, Object> document = new HashMap<>();
            document.put("source_field", "89.160.20.128");
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

            GeoIpProcessor.DatabaseUnavailableProcessor processor = (GeoIpProcessor.DatabaseUnavailableProcessor) factory.create(
                null,
                null,
                null,
                config
            );
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getSourceAndMetadata().get("geoip"), nullValue());
            assertThat(
                ingestDocument.getSourceAndMetadata().get("tags"),
                equalTo(List.of("_geoip_database_unavailable_GeoLite2-City.mmdb"))
            );
        }

        copyDatabase("GeoLite2-City-Test.mmdb", geoipTmpDir);
        databaseNodeService.updateDatabase("GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));

        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "source_field");
            config.put("database_file", "GeoLite2-City.mmdb");

            Map<String, Object> document = new HashMap<>();
            document.put("source_field", "89.160.20.128");
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

            GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getSourceAndMetadata().get("tags"), nullValue());
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData.get("city_name"), equalTo("Linköping"));
        }
    }

    private static void cleanDatabases(final Path directory, ConfigDatabases configDatabases) {
        for (final String database : DEFAULT_DATABASES) {
            configDatabases.updateDatabase(directory.resolve(database), false);
        }
    }
}
