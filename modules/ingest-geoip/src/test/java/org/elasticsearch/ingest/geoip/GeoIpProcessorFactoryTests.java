/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
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
        copyDatabaseFiles(geoIpConfigDir, configDatabases);
        geoipTmpDir = createTempDir();
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        databaseNodeService = new DatabaseNodeService(geoipTmpDir, client, cache, configDatabases, Runnable::run, clusterService);
        databaseNodeService.initialize("nodeId", mock(ResourceWatcherService.class), mock(IngestService.class));
    }

    @After
    public void closeDatabaseReaders() throws IOException {
        databaseNodeService.close();
        databaseNodeService = null;
    }

    public void testBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCountryBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testAsnBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-ASN"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_ASN_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildWithCountryDbAndAsnFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        EnumSet<GeoIpProcessor.Property> asnOnlyProperties = EnumSet.copyOf(GeoIpProcessor.Property.ALL_ASN_PROPERTIES);
        asnOnlyProperties.remove(GeoIpProcessor.Property.IP);
        String asnProperty = RandomPicks.randomFrom(Randomness.get(), asnOnlyProperties).toString();
        config.put("properties", Collections.singletonList(asnProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(
            e.getMessage(),
            equalTo(
                "[properties] illegal property value ["
                    + asnProperty
                    + "]. valid values are [IP, COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_NAME]"
            )
        );
    }

    public void testBuildWithAsnDbAndCityFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        EnumSet<GeoIpProcessor.Property> cityOnlyProperties = EnumSet.copyOf(GeoIpProcessor.Property.ALL_CITY_PROPERTIES);
        cityOnlyProperties.remove(GeoIpProcessor.Property.IP);
        String cityProperty = RandomPicks.randomFrom(Randomness.get(), cityOnlyProperties).toString();
        config.put("properties", Collections.singletonList(cityProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(
            e.getMessage(),
            equalTo("[properties] illegal property value [" + cityProperty + "]. valid values are [IP, ASN, ORGANIZATION_NAME, NETWORK]")
        );
    }

    public void testBuildNonExistingDbFile() throws Exception {
        Files.copy(
            GeoIpProcessorFactoryTests.class.getResourceAsStream("/GeoLite2-City-Test.mmdb"),
            geoipTmpDir.resolve("GeoLite2-City.mmdb")
        );
        databaseNodeService.updateDatabase("GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City.mmdb"));
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "does-not-exist.mmdb");
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[database_file] database file [does-not-exist.mmdb] doesn't exist"));
    }

    public void testBuildBuiltinDatabaseMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        cleanDatabaseFiles(geoIpConfigDir, configDatabases);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", randomFrom(IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES));
        Processor processor = factory.create(null, null, null, config);
        assertThat(processor, instanceOf(GeoIpProcessor.DatabaseUnavailableProcessor.class));
    }

    public void testBuildFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Set<GeoIpProcessor.Property> properties = EnumSet.noneOf(GeoIpProcessor.Property.class);
        List<String> fieldNames = new ArrayList<>();

        int counter = 0;
        int numFields = scaledRandomIntBetween(1, GeoIpProcessor.Property.values().length);
        for (GeoIpProcessor.Property property : GeoIpProcessor.Property.ALL_CITY_PROPERTIES) {
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

    public void testBuildIllegalFieldOption() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("field", "_field");
        config1.put("properties", Collections.singletonList("invalid"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1));
        assertThat(
            e.getMessage(),
            equalTo(
                "[properties] illegal property value [invalid]. valid values are [IP, COUNTRY_ISO_CODE, "
                    + "COUNTRY_NAME, CONTINENT_NAME, REGION_ISO_CODE, REGION_NAME, CITY_NAME, TIMEZONE, LOCATION]"
            )
        );

        Map<String, Object> config2 = new HashMap<>();
        config2.put("field", "_field");
        config2.put("properties", "invalid");
        e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config2));
        assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
    }

    @SuppressWarnings("HiddenField")
    public void testLazyLoading() throws Exception {
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        GeoIpCache cache = new GeoIpCache(1000);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        copyDatabaseFiles(geoIpConfigDir, configDatabases);

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
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        for (DatabaseReaderLazyLoader lazyLoader : configDatabases.getConfigDatabases().values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Collections.singletonMap("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", 1L, "routing", VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-City.mmdb");
        final GeoIpProcessor city = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-City.mmdb").databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-City.mmdb").databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        final GeoIpProcessor country = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-Country.mmdb").databaseReader.get());
        country.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-Country.mmdb").databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        final GeoIpProcessor asn = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-ASN.mmdb").databaseReader.get());
        asn.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoLite2-ASN.mmdb").databaseReader.get());
    }

    @SuppressWarnings("HiddenField")
    public void testLoadingCustomDatabase() throws IOException {
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, new GeoIpCache(1000));
        copyDatabaseFiles(geoIpConfigDir, configDatabases);
        // fake the GeoIP2-City database
        copyDatabaseFile(geoIpConfigDir, "GeoLite2-City.mmdb");
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
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        for (DatabaseReaderLazyLoader lazyLoader : configDatabases.getConfigDatabases().values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Collections.singletonMap("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", 1L, "routing", VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoIP2-City.mmdb");
        final GeoIpProcessor city = (GeoIpProcessor) factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoIP2-City.mmdb").databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseNodeService.getDatabaseReaderLazyLoader("GeoIP2-City.mmdb").databaseReader.get());
        resourceWatcherService.close();
        threadPool.shutdown();
    }

    public void testFallbackUsingDefaultDatabases() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("fallback_to_default_databases", randomBoolean());
        factory.create(null, null, null, config);
        assertWarnings(GeoIpProcessor.DEFAULT_DATABASES_DEPRECATION_MESSAGE);
    }

    public void testDownloadDatabaseOnPipelineCreation() throws IOException {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
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
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config);

        processor.execute(RandomDocumentPicks.randomIngestDocument(random(), Map.of("_field", "89.160.20.128")));
    }

    public void testUpdateDatabaseWhileIngesting() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
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
            copyDatabaseFile(geoipTmpDir, "GeoLite2-City-Test.mmdb");
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            databaseNodeService.updateDatabase("GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));
            processor.execute(ingestDocument);
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData.get("city_name"), equalTo("Linköping"));
        }
        {
            // No databases are available, so assume that databases still need to be downloaded and therefor not fail:
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            databaseNodeService.removeStaleEntries(List.of("GeoLite2-City.mmdb"));
            configDatabases.updateDatabase(geoIpConfigDir.resolve("GeoLite2-City.mmdb"), false);
            processor.execute(ingestDocument);
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData, nullValue());
        }
        {
            // There are database available, but not the right one, so tag:
            databaseNodeService.updateDatabase("GeoLite2-City-Test.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.getSourceAndMetadata(), hasEntry("tags", List.of("_geoip_database_unavailable_GeoLite2-City.mmdb")));
        }
    }

    public void testDatabaseNotReadyYet() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseNodeService);
        cleanDatabaseFiles(geoIpConfigDir, configDatabases);

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

        copyDatabaseFile(geoipTmpDir, "GeoLite2-City-Test.mmdb");
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

    private static void copyDatabaseFile(final Path path, final String databaseFilename) throws IOException {
        Files.copy(
            new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/" + databaseFilename)),
            path.resolve(databaseFilename),
            StandardCopyOption.REPLACE_EXISTING
        );
    }

    static void copyDatabaseFiles(final Path path, ConfigDatabases configDatabases) throws IOException {
        for (final String databaseFilename : IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES) {
            copyDatabaseFile(path, databaseFilename);
            configDatabases.updateDatabase(path.resolve(databaseFilename), true);
        }
    }

    static void cleanDatabaseFiles(final Path path, ConfigDatabases configDatabases) throws IOException {
        for (final String databaseFilename : IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES) {
            configDatabases.updateDatabase(path.resolve(databaseFilename), false);
        }
    }

}
