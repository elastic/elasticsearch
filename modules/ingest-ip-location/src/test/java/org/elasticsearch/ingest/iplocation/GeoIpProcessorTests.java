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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.geoip.DatabaseNodeService;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.CLASSIC;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.FLEXIBLE;
import static org.elasticsearch.ingest.IngestPipelineTestUtils.runWithAccessPattern;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.createTestDatabaseNodeServiceFromExistingDir;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.IP_LOCATION_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class GeoIpProcessorTests extends ESTestCase {

    /**
     * The set of databases registered on the shared config dir; mirrors the on-disk basenames so
     * {@link GeoIpProcessorTests#setup()} can register them without re-copying.
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
        copyDatabase("ipinfo/ip_geolocation_standard_sample.mmdb", sharedGeoIpConfigDir.resolve("ip_geolocation_standard_sample.mmdb"));
    }

    @Before
    public void setup() throws IOException {
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
    public void cleanup() throws IOException {
        databaseNodeService.shutdown();
    }

    /**
     * Creates a lookup with ALL valid properties for the database, matching former approach
     * which used {@code database.properties()} rather than {@code database.defaultProperties()}.
     */
    private IpDataLookup createLookup(String databaseFile) {
        IpDataLookupInfo info = databaseNodeService.getIpDataLookupInfo(databaseFile);
        List<String> allProperties = new ArrayList<>(info.getFields().keySet());
        return databaseNodeService.createIpDataLookup(projectId.id(), databaseFile, allProperties);
    }

    public void testMaxmindCity() throws Exception {
        String ip = "2602:306:33d3:8000::3257:9652";
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo(ip));
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(data, notNullValue());
        assertThat(data.get("ip"), equalTo(ip));
        assertThat(data.get("city_name"), equalTo("Homestead"));
        // see MaxmindIpDataLookupsTests for more tests of the data lookup behavior
    }

    public void testIpinfoGeolocation() throws Exception {
        String ip = "72.20.12.220";
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("ip_geolocation_standard_sample.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo(ip));
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(data, notNullValue());
        assertThat(data.get("ip"), equalTo(ip));
        assertThat(data.get("city_name"), equalTo("Chicago"));
        // see IpinfoIpDataLookupsTests for more tests of the data lookup behavior
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            true,
            false,
            "filename"
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap("source_field", null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            true,
            false,
            "filename"
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap("source_field", null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot extract geoip information."));
    }

    public void testNonExistentWithoutIgnoreMissing() {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    public void testAddressIsNotInTheDatabase() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "127.0.0.1");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("target_field"), is(false));
    }

    /**
     * Tests that an exception in the IpDataLookup is propagated out of the GeoIpProcessor's execute method
     */
    public void testExceptionPropagates() {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "www.google.com");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("not an IP string literal"));
    }

    public void testListAllValid() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "82.171.64.0"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(data, notNullValue());
        assertThat(data.size(), equalTo(2));
        assertThat(data.get(0).get("location"), equalTo(Map.of("lat", 37.751d, "lon", -97.822d)));
        assertThat(data.get(1).get("city_name"), equalTo("Hoensbroek"));
    }

    public void testListPartiallyValid() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(data, notNullValue());
        assertThat(data.size(), equalTo(2));
        assertThat(data.get(0).get("location"), equalTo(Map.of("lat", 37.751d, "lon", -97.822d)));
        assertThat(data.get(1), nullValue());
    }

    public void testListNoMatches() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("127.0.0.1", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertFalse(ingestDocument.hasField("target_field"));
    }

    public void testListFirstOnly() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(data, notNullValue());
        assertThat(data.get("location"), equalTo(Map.of("lat", 37.751d, "lon", -97.822d)));
    }

    public void testListFirstOnlyNoMatches() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("127.0.0.1", "127.0.0.2"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().containsKey("target_field"), is(false));
    }

    // -- Tests that require mocks (no real DB can simulate these states) --

    public void testInvalidDatabase() throws Exception {
        IpDataLookupInfo info = mockInfo("GeoLite2-City");
        IpDataLookup lookup = mock(IpDataLookup.class);
        when(lookup.isValid()).thenReturn(false);
        when(lookup.getInfo()).thenReturn(info);

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "target_field",
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("127.0.0.1", "127.0.0.2"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().containsKey("target_field"), is(false));
        assertThat(ingestDocument.getSourceAndMetadata(), hasEntry("tags", List.of("_geoip_expired_database")));
    }

    public void testNoDatabase() throws Exception {
        IpDataLookupInfo info = mockInfo("GeoLite2-City");
        IpDataLookup lookup = mock(IpDataLookup.class);
        when(lookup.isValid()).thenReturn(true);
        when(lookup.getInfo()).thenReturn(info);
        when(lookup.lookup(anyString())).thenReturn(null);

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "target_field",
            false,
            false,
            "GeoLite2-City"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "8.8.8.8");
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("target_field"), is(false));
        assertThat(ingestDocument.getSourceAndMetadata(), hasEntry("tags", List.of("_geoip_database_unavailable_GeoLite2-City")));
    }

    public void testNoDatabase_ignoreMissing() throws Exception {
        IpDataLookupInfo info = mockInfo("GeoLite2-City");
        IpDataLookup lookup = mock(IpDataLookup.class);
        when(lookup.isValid()).thenReturn(true);
        when(lookup.getInfo()).thenReturn(info);
        when(lookup.lookup(anyString())).thenReturn(null);

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "target_field",
            true,
            false,
            "GeoLite2-City"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "8.8.8.8");
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    // -- Tests for new branch-only functionality (no main equivalent) --

    public void testInvalidFieldType() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "target_field",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", 42);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("should contain only string or array of strings"));
    }

    public void testTypeGeoip() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );
        assertThat(processor.getType(), equalTo("geoip"));
    }

    public void testTypeIpLocation() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "ip_location",
            false,
            true,
            "GeoLite2-City.mmdb"
        );
        assertThat(processor.getType(), equalTo("ip_location"));
    }

    // -- Flexible and Classic field access mode tests --

    public void testMaxmindCityWithFlexibleFieldAccessMode() throws Exception {
        String ip = "2602:306:33d3:8000::3257:9652";
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "my.target",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with flexible field access mode
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        // Verify that individual dotted fields were created (check in source map directly)
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        assertThat(sourceAndMetadata.get("my.target.ip"), equalTo(ip));
        assertThat(sourceAndMetadata.containsKey("my.target.city_name"), is(true));
        assertThat(sourceAndMetadata.get("my.target.city_name"), equalTo("Homestead"));
        assertThat(sourceAndMetadata.containsKey("my.target.country_name"), is(true));
        assertThat(sourceAndMetadata.containsKey("my.target.country_iso_code"), is(true));
        assertThat(sourceAndMetadata.containsKey("my.target.continent_name"), is(true));
        assertThat(sourceAndMetadata.containsKey("my.target.region_name"), is(true));
        assertThat(sourceAndMetadata.containsKey("my.target.region_iso_code"), is(true));

        // Verify that location is written as an array [lon, lat] in flexible mode
        assertThat(sourceAndMetadata.containsKey("my.target.location"), is(true));
        Object location = sourceAndMetadata.get("my.target.location");
        assertThat(location, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Double> locationArray = (List<Double>) location;
        assertThat(locationArray.size(), equalTo(2));
        assertThat(locationArray.get(0), equalTo(-80.4572));
        assertThat(locationArray.get(1), equalTo(25.4573));

        // Verify that the nested "my.target" object was NOT created
        assertThat(sourceAndMetadata.containsKey("my.target"), is(false));
    }

    public void testMaxmindCityWithClassicFieldAccessMode() throws Exception {
        String ip = "2602:306:33d3:8000::3257:9652";
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "my.target",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with classic field access mode (default)
        ingestDocument = runWithAccessPattern(CLASSIC, ingestDocument, processor);

        // Verify that a nested object was created (classic behavior)
        assertThat(ingestDocument.hasField("my.target"), is(true));
        Object target = ingestDocument.getFieldValue("my.target", Object.class);
        assertThat(target, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) target;
        assertThat(data, notNullValue());
        assertThat(data.get("ip"), equalTo(ip));
        assertThat(data.get("city_name"), equalTo("Homestead"));

        @SuppressWarnings("unchecked")
        Map<String, Object> locationMap = (Map<String, Object>) data.get("location");
        assertThat(locationMap, notNullValue());
        assertThat(locationMap, hasEntry("lat", 25.4573));
        assertThat(locationMap, hasEntry("lon", -80.4572));
    }

    public void testIpinfoGeolocationWithFlexibleFieldAccessMode() throws Exception {
        String ip = "72.20.12.220";
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("ip_geolocation_standard_sample.mmdb"),
            "my.geo",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with flexible field access mode
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        // Verify that individual dotted fields were created (check in source map directly)
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("my.geo.ip"), is(true));
        assertThat(sourceAndMetadata.get("my.geo.ip"), equalTo(ip));
        assertThat(sourceAndMetadata.containsKey("my.geo.city_name"), is(true));
        assertThat(sourceAndMetadata.get("my.geo.city_name"), equalTo("Chicago"));
        assertThat(sourceAndMetadata.containsKey("my.geo.country_iso_code"), is(true));

        // Verify that location is written as an array [lon, lat] in flexible mode
        assertThat(sourceAndMetadata.containsKey("my.geo.location"), is(true));
        Object location = sourceAndMetadata.get("my.geo.location");
        assertThat(location, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Double> locationArray = (List<Double>) location;
        assertThat(locationArray.size(), equalTo(2));
        assertThat(locationArray.get(0), equalTo(-87.6285));
        assertThat(locationArray.get(1), equalTo(41.8798));

        // Verify that the nested "my.geo" object was NOT created
        assertThat(sourceAndMetadata.containsKey("my.geo"), is(false));
    }

    public void testIpinfoGeolocationWithClassicFieldAccessMode() throws Exception {
        String ip = "72.20.12.220";
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("ip_geolocation_standard_sample.mmdb"),
            "my.geo",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with classic field access mode
        ingestDocument = runWithAccessPattern(CLASSIC, ingestDocument, processor);

        // Verify that a nested object was created (classic behavior)
        assertThat(ingestDocument.hasField("my.geo"), is(true));
        Object target = ingestDocument.getFieldValue("my.geo", Object.class);
        assertThat(target, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) target;
        assertThat(data, notNullValue());
        assertThat(data.get("ip"), equalTo(ip));
        assertThat(data.get("city_name"), equalTo("Chicago"));
    }

    public void testArrayWithFlexibleFieldAccessModeFirstOnly() throws Exception {
        String ip1 = "2602:306:33d3:8000::3257:9652";
        String ip2 = "82.170.213.79";
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "my.target",
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of(ip1, ip2));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with flexible field access mode
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        // Verify that individual dotted fields were created for the first IP only (check in source map directly)
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        assertThat(sourceAndMetadata.get("my.target.ip"), equalTo(ip1));
        assertThat(sourceAndMetadata.containsKey("my.target.city_name"), is(true));
        assertThat(sourceAndMetadata.get("my.target.city_name"), equalTo("Homestead"));
    }

    public void testIpLocationAsnWithFlexibleFieldAccessMode() throws Exception {
        String ip = "82.171.64.0";
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-ASN.mmdb"),
            "ip.asn",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with flexible field access mode
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        // Verify that individual dotted fields were created (check in source map directly)
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("ip.asn.ip"), is(true));
        assertThat(sourceAndMetadata.get("ip.asn.ip"), equalTo(ip));
        assertThat(sourceAndMetadata.containsKey("ip.asn.asn"), is(true));
        assertThat(sourceAndMetadata.containsKey("ip.asn.organization_name"), is(true));
        assertThat(sourceAndMetadata.containsKey("ip.asn.network"), is(true));

        // Verify that the nested "ip.asn" object was NOT created
        assertThat(sourceAndMetadata.containsKey("ip.asn"), is(false));
    }

    public void testIpLocationCountryWithFlexibleFieldAccessMode() throws Exception {
        String ip = "82.170.213.79";
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "ip_address",
            createLookup("GeoLite2-Country.mmdb"),
            "geo.country",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("ip_address", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with flexible field access mode
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        // Verify that individual dotted fields were created (check in source map directly)
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("geo.country.ip"), is(true));
        assertThat(sourceAndMetadata.get("geo.country.ip"), equalTo(ip));
        assertThat(sourceAndMetadata.containsKey("geo.country.country_iso_code"), is(true));
        assertThat(sourceAndMetadata.get("geo.country.country_iso_code"), equalTo("NL"));
        assertThat(sourceAndMetadata.containsKey("geo.country.country_name"), is(true));
        assertThat(sourceAndMetadata.get("geo.country.country_name"), equalTo("Netherlands"));
        assertThat(sourceAndMetadata.containsKey("geo.country.continent_name"), is(true));
        assertThat(sourceAndMetadata.get("geo.country.continent_name"), equalTo("Europe"));

        // Verify that the nested "geo.country" object was NOT created
        assertThat(sourceAndMetadata.containsKey("geo.country"), is(false));
    }

    // Test that both geoip and ip_location processors work correctly with flexible mode
    public void testBothProcessorTypesWithFlexibleMode() throws Exception {
        String ip = "2602:306:33d3:8000::3257:9652";

        // Test with GEOIP_TYPE
        GeoIpProcessor geoipProcessor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "ip",
            createLookup("GeoLite2-City.mmdb"),
            "geoip.result",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("ip", ip);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, geoipProcessor);

        // Verify geoip processor created dotted fields (check in source map directly)
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("geoip.result.city_name"), is(true));
        assertThat(sourceAndMetadata.get("geoip.result.city_name"), equalTo("Homestead"));
        assertThat(sourceAndMetadata.containsKey("geoip.result.location"), is(true));
        assertThat(sourceAndMetadata.containsKey("geoip.result"), is(false));

        // Test with IP_LOCATION_TYPE
        GeoIpProcessor ipLocationProcessor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "ip",
            createLookup("GeoLite2-City.mmdb"),
            "ip_location.result",
            false,
            false,
            "filename"
        );

        document = new HashMap<>();
        document.put("ip", ip);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, ipLocationProcessor);

        // Verify ip_location processor created dotted fields (check in source map directly)
        sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("ip_location.result.city_name"), is(true));
        assertThat(sourceAndMetadata.get("ip_location.result.city_name"), equalTo("Homestead"));
        assertThat(sourceAndMetadata.containsKey("ip_location.result.location"), is(true));
        assertThat(sourceAndMetadata.containsKey("ip_location.result"), is(false));
    }

    public void testListWithFlexibleFieldAccessMode() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "my.target",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "82.171.64.0"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with flexible field access mode
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        // Verify that individual dotted fields contain lists (one value per IP)
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();

        // Check that location is a list of arrays
        assertThat(sourceAndMetadata.containsKey("my.target.location"), is(true));
        Object location = sourceAndMetadata.get("my.target.location");
        assertThat(location, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<List<Double>> locationList = (List<List<Double>>) location;
        assertThat(locationList.size(), equalTo(2));
        // First IP: 8.8.8.8
        assertThat(locationList.get(0).get(0), equalTo(-97.822d));
        assertThat(locationList.get(0).get(1), equalTo(37.751d));
        // Second IP: 82.171.64.0
        assertThat(locationList.get(1).get(0), equalTo(5.9345d));
        assertThat(locationList.get(1).get(1), equalTo(50.9118d));

        // Check that city_name is a list of strings
        assertThat(sourceAndMetadata.containsKey("my.target.city_name"), is(true));
        Object cityName = sourceAndMetadata.get("my.target.city_name");
        assertThat(cityName, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> cityNameList = (List<String>) cityName;
        assertThat(cityNameList.size(), equalTo(2));
        assertThat(cityNameList.get(1), equalTo("Hoensbroek"));

        // Check that ip is a list of strings
        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        Object ip = sourceAndMetadata.get("my.target.ip");
        assertThat(ip, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> ipList = (List<String>) ip;
        assertThat(ipList.size(), equalTo(2));
        assertThat(ipList.get(0), equalTo("8.8.8.8"));
        assertThat(ipList.get(1), equalTo("82.171.64.0"));

        // Verify that the nested "my.target" object was NOT created
        assertThat(sourceAndMetadata.containsKey("my.target"), is(false));
    }

    public void testListPartiallyValidWithFlexibleFieldAccessMode() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            createLookup("GeoLite2-City.mmdb"),
            "my.target",
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Execute with flexible field access mode
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        // Verify that individual dotted fields contain lists with nulls for non-matching IPs
        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();

        // Check that location is a list with one valid location and one null
        assertThat(sourceAndMetadata.containsKey("my.target.location"), is(true));
        Object location = sourceAndMetadata.get("my.target.location");
        assertThat(location, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> locationList = (List<Object>) location;
        assertThat(locationList.size(), equalTo(2));
        assertThat(locationList.get(0), instanceOf(List.class));
        assertThat(locationList.get(1), nullValue());

        // Check that ip list has the successful IP and null for the failed one
        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        Object ip = sourceAndMetadata.get("my.target.ip");
        assertThat(ip, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> ipList = (List<String>) ip;
        assertThat(ipList.size(), equalTo(2));
        assertThat(ipList.get(0), equalTo("8.8.8.8"));
        assertThat(ipList.get(1), nullValue());
    }

    private static IpDataLookupInfo mockInfo(String databaseType) {
        IpDataLookupInfo info = mock(IpDataLookupInfo.class);
        when(info.getDatabaseType()).thenReturn(databaseType);
        SequencedMap<String, Class<?>> fields = new LinkedHashMap<>();
        fields.put("city_name", String.class);
        fields.put("country_name", String.class);
        when(info.getFields()).thenReturn(fields);
        return info;
    }
}
