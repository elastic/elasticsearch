/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.geoip.DatabaseNodeService;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SequencedMap;
import java.util.Set;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.createTestDatabaseNodeServiceFromExistingDir;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.IP_LOCATION_TYPE;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class GeoIpProcessorFactoryTests extends ESTestCase {

    /**
     * The set of databases registered on the shared config dir; mirrors the on-disk basenames so
     * {@link #loadDatabaseReaders()} can register them without re-copying.
     */
    private static final String[] REGISTERED_DATABASES = {
        "GeoLite2-ASN.mmdb",
        "GeoLite2-City.mmdb",
        "GeoLite2-Country.mmdb",
        "ip_geolocation_standard_sample.mmdb" };

    /**
     * A class-scoped read-only directory holding every mmdb this suite consumes.
     * Tests in this class never mutate the on-disk databases, so a single shared directory is safe.
     */
    private static Path sharedGeoIpConfigDir;

    private DatabaseNodeService databaseNodeService;
    private ProjectId projectId;

    @BeforeClass
    public static void setupSharedGeoIpConfigDir() throws IOException {
        sharedGeoIpConfigDir = createTempDir().resolve("ingest-geoip");
        Files.createDirectories(sharedGeoIpConfigDir);
        copyDefaultDatabases(sharedGeoIpConfigDir);
        // copyDatabase treats a directory destination as `<dir>/<resourceName>`, which would create
        // an `ipinfo/...` subpath that doesn't exist; pass the explicit basename target instead.
        copyDatabase("ipinfo/ip_geolocation_standard_sample.mmdb", sharedGeoIpConfigDir.resolve("ip_geolocation_standard_sample.mmdb"));
    }

    @Before
    public void loadDatabaseReaders() throws IOException {
        // cover for multi-project enable/disabled
        boolean multiProject = randomBoolean();
        projectId = multiProject ? randomProjectIdOrDefault() : ProjectId.DEFAULT;
        ProjectResolver projectResolver = multiProject
            ? TestProjectResolvers.singleProject(projectId)
            : TestProjectResolvers.DEFAULT_PROJECT_ONLY;

        databaseNodeService = createTestDatabaseNodeServiceFromExistingDir(
            sharedGeoIpConfigDir,
            createTempDir(),
            projectId,
            projectResolver,
            REGISTERED_DATABASES
        );
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

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config, projectId);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, processorTag, null, config, projectId);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config, projectId);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config, projectId);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
    }

    public void testBuildWithProperties() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        List<String> allCityProperties = List.copyOf(
            Objects.requireNonNull(databaseNodeService.getIpDataLookupInfo("GeoLite2-City.mmdb")).getFields().keySet()
        );
        List<String> selectedProperties = randomSubsetOf(randomIntBetween(1, allCityProperties.size()), allCityProperties);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", selectedProperties);
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(null, null, null, config, projectId);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties().keySet(), equalTo(Set.copyOf(selectedProperties)));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildIllegalFieldOption() {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("field", "_field");
        config1.put("properties", List.of("invalid"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1, projectId));
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
        e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config2, projectId));
        assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
    }

    public void testBuildNonExistingDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "does-not-exist.mmdb");
        Processor processor = factory.create(null, null, null, config, projectId);
        assertThat(processor, instanceOf(DatabaseUnavailableProcessor.class));
    }

    public void testBuildMissingField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, "_tag", null, config, projectId)
        );
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testDownloadDatabaseOnPipelineCreation() throws IOException {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomIdentifier());
        config.put("download_database_on_pipeline_creation", randomBoolean());
        factory.create(null, null, null, config, projectId);
        // Check all the config params were consumed.
        assertThat(config, anEmptyMap());
    }

    public void testDownloadDatabaseOnPipelineCreationStaticHelper() {
        Map<String, Object> config = new HashMap<>();
        config.put("download_database_on_pipeline_creation", false);
        assertFalse(GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(config));

        config.put("download_database_on_pipeline_creation", true);
        assertTrue(GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(config));

        assertTrue(GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(Map.of()));
    }

    public void testBuildIpLocationType() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(IP_LOCATION_TYPE, databaseNodeService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("database_file", "ip_geolocation_standard_sample.mmdb");

        Processor processor = factory.create(null, "_tag", null, config, projectId);
        assertThat(processor, instanceOf(GeoIpProcessor.class));
        assertThat(processor.getType(), equalTo("ip_location"));
    }

    public void testDatabaseNotReadyYet() throws Exception {
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(null);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("database_file", "GeoLite2-City.mmdb");

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "89.160.20.128");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        DatabaseUnavailableProcessor processor = (DatabaseUnavailableProcessor) factory.create(null, null, null, config, ProjectId.DEFAULT);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().get("geoip"), nullValue());
        assertThat(ingestDocument.getSourceAndMetadata().get("tags"), equalTo(List.of("_geoip_database_unavailable_GeoLite2-City.mmdb")));
    }

    public void testBuildUnsupportedDatabase() throws Exception {
        // mock up a lookup from a database with an unsupported type
        IpDataLookup lookup = mockLookup("IPinfo standard_asn");
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("database_file", "asn.mmdb");

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, "_tag", null, config, ProjectId.DEFAULT)
        );
        assertThat(e.getMessage(), containsString("Unsupported database type"));
    }

    public void testBuildNullDatabaseType() throws Exception {
        // mock up a lookup that returns a null databaseType
        IpDataLookup lookup = mockLookup(null);
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", List.of("ip"));

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, ProjectId.DEFAULT)
        );
        assertThat(e.getMessage(), equalTo("[database_file] Unsupported database type [null] for file [GeoLite2-City.mmdb]"));
    }

    public void testStrictMaxmindSupport() throws Exception {
        for (String databaseType : List.of("ipinfo some_ipinfo_database.mmdb-City", "ipinfo_free_country.mmdb", "ipinfo")) {
            IpDataLookup lookup = mockLookup(databaseType);
            IpLocationService service = mock(IpLocationService.class);
            when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(lookup);

            GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);

            Map<String, Object> config = new HashMap<>();
            config.put("field", "source_field");
            config.put("database_file", "some-ipinfo-database.mmdb");

            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> factory.create(null, null, null, config, ProjectId.DEFAULT)
            );
            assertThat(
                e.getMessage(),
                equalTo("[database_file] Unsupported database type [" + databaseType + "] for file [some-ipinfo-database.mmdb]")
            );
        }
    }

    public void testLaxMaxmindSupport() throws Exception {
        IpDataLookup lookup = mockLookup("some_custom_database.mmdb-City");
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("database_file", "some-custom-database.mmdb");

        factory.create(null, null, null, config, ProjectId.DEFAULT);
        assertWarnings(GeoIpProcessor.UNSUPPORTED_DATABASE_DEPRECATION_MESSAGE.replaceAll("\\{}", "some_custom_database.mmdb-City"));
    }

    private static IpDataLookup mockLookup(String databaseType) {
        IpDataLookupInfo info = mock(IpDataLookupInfo.class);
        when(info.getDatabaseType()).thenReturn(databaseType);
        SequencedMap<String, Class<?>> fields = new LinkedHashMap<>();
        fields.put("city_name", String.class);
        when(info.getFields()).thenReturn(fields);

        IpDataLookup lookup = mock(IpDataLookup.class);
        when(lookup.isValid()).thenReturn(true);
        when(lookup.getInfo()).thenReturn(info);
        return lookup;
    }
}
