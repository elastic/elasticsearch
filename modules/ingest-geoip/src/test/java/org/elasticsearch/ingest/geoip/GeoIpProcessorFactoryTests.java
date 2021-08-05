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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpProcessorFactoryTests extends ESTestCase {

    private Path geoipTmpDir;
    private DatabaseRegistry databaseRegistry;
    private ClusterService clusterService;

    @Before
    public void loadDatabaseReaders() throws IOException {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        copyDatabaseFiles(geoIpDir);

        Client client = mock(Client.class);
        GeoIpCache cache = new GeoIpCache(1000);
        LocalDatabases localDatabases = new LocalDatabases(geoIpDir, geoIpConfigDir, new GeoIpCache(1000));
        geoipTmpDir = createTempDir();
        databaseRegistry = new DatabaseRegistry(geoipTmpDir, client, cache, localDatabases, Runnable::run);
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
    }

    @After
    public void closeDatabaseReaders() throws IOException {
        databaseRegistry.close();
        databaseRegistry = null;
    }

    public void testBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCountryBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testAsnBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-ASN"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_ASN_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        GeoIpProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        GeoIpProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildWithCountryDbAndAsnFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        EnumSet<GeoIpProcessor.Property> asnOnlyProperties = EnumSet.copyOf(GeoIpProcessor.Property.ALL_ASN_PROPERTIES);
        asnOnlyProperties.remove(GeoIpProcessor.Property.IP);
        String asnProperty = RandomPicks.randomFrom(Randomness.get(), asnOnlyProperties).toString();
        config.put("properties", Collections.singletonList(asnProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[properties] illegal property value [" + asnProperty +
            "]. valid values are [IP, COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_NAME]"));
    }

    public void testBuildWithAsnDbAndCityFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        EnumSet<GeoIpProcessor.Property> cityOnlyProperties = EnumSet.copyOf(GeoIpProcessor.Property.ALL_CITY_PROPERTIES);
        cityOnlyProperties.remove(GeoIpProcessor.Property.IP);
        String cityProperty = RandomPicks.randomFrom(Randomness.get(), cityOnlyProperties).toString();
        config.put("properties", Collections.singletonList(cityProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[properties] illegal property value [" + cityProperty +
            "]. valid values are [IP, ASN, ORGANIZATION_NAME, NETWORK]"));
    }

    public void testBuildNonExistingDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "does-not-exist.mmdb");
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[database_file] database file [does-not-exist.mmdb] doesn't exist"));
    }

    public void testBuildFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

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
        GeoIpProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(properties));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildIllegalFieldOption() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("field", "_field");
        config1.put("properties", Collections.singletonList("invalid"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1));
        assertThat(e.getMessage(), equalTo("[properties] illegal property value [invalid]. valid values are [IP, COUNTRY_ISO_CODE, " +
            "COUNTRY_NAME, CONTINENT_NAME, REGION_ISO_CODE, REGION_NAME, CITY_NAME, TIMEZONE, LOCATION]"));

        Map<String, Object> config2 = new HashMap<>();
        config2.put("field", "_field");
        config2.put("properties", "invalid");
        e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config2));
        assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
    }

    public void testLazyLoading() throws Exception {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        copyDatabaseFiles(geoIpDir);

        // Loading another database reader instances, because otherwise we can't test lazy loading as the
        // database readers used at class level are reused between tests. (we want to keep that otherwise running this
        // test will take roughly 4 times more time)
        Client client = mock(Client.class);
        GeoIpCache cache = new GeoIpCache(1000);
        LocalDatabases localDatabases = new LocalDatabases(geoIpDir, geoIpConfigDir, cache);
        DatabaseRegistry databaseRegistry = new DatabaseRegistry(createTempDir(), client, cache, localDatabases, Runnable::run);
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        for (DatabaseReaderLazyLoader lazyLoader : localDatabases.getAllDatabases()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Collections.singletonMap("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", "routing", 1L, VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-City.mmdb");
        final GeoIpProcessor city = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseRegistry.getDatabase("GeoLite2-City.mmdb", true).databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseRegistry.getDatabase("GeoLite2-City.mmdb", true).databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        final GeoIpProcessor country = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseRegistry.getDatabase("GeoLite2-Country.mmdb", true).databaseReader.get());
        country.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseRegistry.getDatabase("GeoLite2-Country.mmdb", true).databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        final GeoIpProcessor asn = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseRegistry.getDatabase("GeoLite2-ASN.mmdb", true).databaseReader.get());
        asn.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseRegistry.getDatabase("GeoLite2-ASN.mmdb", true).databaseReader.get());
    }

    public void testLoadingCustomDatabase() throws IOException {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        copyDatabaseFiles(geoIpDir);
        // fake the GeoIP2-City database
        copyDatabaseFile(geoIpConfigDir, "GeoLite2-City.mmdb");
        Files.move(geoIpConfigDir.resolve("GeoLite2-City.mmdb"), geoIpConfigDir.resolve("GeoIP2-City.mmdb"));

        /*
         * Loading another database reader instances, because otherwise we can't test lazy loading as the database readers used at class
         * level are reused between tests. (we want to keep that otherwise running this test will take roughly 4 times more time).
         */
        ThreadPool threadPool = new TestThreadPool("test");
        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        LocalDatabases localDatabases = new LocalDatabases(geoIpDir, geoIpConfigDir, new GeoIpCache(1000));
        Client client = mock(Client.class);
        GeoIpCache cache = new GeoIpCache(1000);
        DatabaseRegistry databaseRegistry = new DatabaseRegistry(createTempDir(), client, cache, localDatabases, Runnable::run);
        databaseRegistry.initialize("nodeId", resourceWatcherService, mock(IngestService.class));
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        for (DatabaseReaderLazyLoader lazyLoader : localDatabases.getAllDatabases()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Collections.singletonMap("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", "routing", 1L, VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoIP2-City.mmdb");
        final GeoIpProcessor city = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseRegistry.getDatabase("GeoIP2-City.mmdb", true).databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseRegistry.getDatabase("GeoIP2-City.mmdb", true).databaseReader.get());
        resourceWatcherService.close();
        threadPool.shutdown();
    }

    public void testFallbackUsingDefaultDatabases() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "source_field");
            config.put("fallback_to_default_databases", false);
            Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
            assertThat(e.getMessage(), equalTo("[database_file] database file [GeoLite2-City.mmdb] doesn't exist"));
        }
        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "source_field");
            if (randomBoolean()) {
                config.put("fallback_to_default_databases", true);
            }
            GeoIpProcessor processor = factory.create(null, null, null, config);
            assertThat(processor, notNullValue());
        }
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
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);

        processor.execute(RandomDocumentPicks.randomIngestDocument(random(), Map.of("_field", "89.160.20.128")));
    }

    public void testFallbackUsingDefaultDatabasesWhileIngesting() throws Exception {
        copyDatabaseFile(geoipTmpDir, "GeoLite2-City-Test.mmdb");
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseRegistry, clusterService);
        // fallback_to_default_databases=true, first use default city db then a custom city db:
        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "source_field");
            if (randomBoolean()) {
                config.put("fallback_to_default_databases", true);
            }
            GeoIpProcessor processor = factory.create(null, null, null, config);
            Map<String, Object> document = new HashMap<>();
            document.put("source_field", "89.160.20.128");
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData.get("city_name"), equalTo("Tumba"));

            databaseRegistry.updateDatabase("GeoLite2-City.mmdb", "md5", geoipTmpDir.resolve("GeoLite2-City-Test.mmdb"));
            ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData.get("city_name"), equalTo("Linköping"));
        }
        // fallback_to_default_databases=false, first use a custom city db then remove the custom db and expect failure:
        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "source_field");
            config.put("fallback_to_default_databases", false);
            GeoIpProcessor processor = factory.create(null, null, null, config);
            Map<String, Object> document = new HashMap<>();
            document.put("source_field", "89.160.20.128");
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            Map<?, ?> geoData = (Map<?, ?>) ingestDocument.getSourceAndMetadata().get("geoip");
            assertThat(geoData.get("city_name"), equalTo("Linköping"));
            databaseRegistry.removeStaleEntries(List.of("GeoLite2-City.mmdb"));
            Exception e = expectThrows(ResourceNotFoundException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("database file [GeoLite2-City.mmdb] doesn't exist"));
        }
    }

    private static void copyDatabaseFile(final Path path, final String databaseFilename) throws IOException {
        Files.copy(
            new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/" + databaseFilename)),
            path.resolve(databaseFilename)
        );
    }

    static void copyDatabaseFiles(final Path path) throws IOException {
        for (final String databaseFilename : IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES) {
            copyDatabaseFile(path, databaseFilename);
        }
    }

}
