/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.CLASSIC;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.FLEXIBLE;
import static org.elasticsearch.ingest.IngestPipelineTestUtils.runWithAccessPattern;
import static org.elasticsearch.ingest.geoip.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.geoip.GeoIpProcessor.IP_LOCATION_TYPE;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoIpProcessorTests extends ESTestCase {

    // a temporary directory that mmdb files can be copied to and read from
    private Path tmpDir;

    @Before
    public void setup() {
        tmpDir = createTempDir();
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.rm(tmpDir);
    }

    public void testMaxmindCity() throws Exception {
        String ip = "2602:306:33d3:8000::3257:9652";
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE, // n.b. this is a "geoip" processor
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            IP_LOCATION_TYPE, // n.b. this is an "ip_location" processor
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("ipinfo/ip_geolocation_standard_sample.mmdb"),
            () -> true,
            "target_field",
            getIpinfoGeolocationLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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

    public void testListDatabaseReferenceCounting() throws Exception {
        AtomicBoolean closeCheck = new AtomicBoolean(false);
        var loader = loader("GeoLite2-City.mmdb", closeCheck);
        GeoIpProcessor processor = new GeoIpProcessor(GEOIP_TYPE, randomAlphaOfLength(10), null, "source_field", () -> {
            loader.preLookup();
            return loader;
        }, () -> true, "target_field", getMaxmindCityLookup(), false, false, "filename");

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

        // Check the loader's reference count and attempt to close
        assertThat(loader.current(), equalTo(0));
        loader.shutdown();
        assertTrue(closeCheck.get());
    }

    public void testListFirstOnly() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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

    public void testInvalidDatabase() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("GeoLite2-City.mmdb"),
            () -> false,
            "target_field",
            getMaxmindCityLookup(),
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
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            () -> null,
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            () -> null,
            () -> true,
            "target_field",
            getMaxmindCityLookup(),
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

    private static IpDataLookup getMaxmindCityLookup() {
        final var database = Database.City;
        return MaxmindIpDataLookups.getMaxmindLookup(database).apply(database.properties());
    }

    private static IpDataLookup getMaxmindAsnLookup() {
        final var database = Database.Asn;
        return MaxmindIpDataLookups.getMaxmindLookup(database).apply(database.properties());
    }

    private static IpDataLookup getMaxmindCountryLookup() {
        final var database = Database.Country;
        return MaxmindIpDataLookups.getMaxmindLookup(database).apply(database.properties());
    }

    private static IpDataLookup getIpinfoGeolocationLookup() {
        final var database = Database.CityV2;
        return IpinfoIpDataLookups.getIpinfoLookup(database).apply(database.properties());
    }

    private CheckedSupplier<IpDatabase, IOException> loader(final String path) {
        var loader = loader(path, null);
        return () -> loader;
    }

    @FixForMultiProject(description = "Replace DEFAULT project")
    private DatabaseReaderLazyLoader loader(final String databaseName, final AtomicBoolean closed) {
        int last = databaseName.lastIndexOf("/");
        final Path path = tmpDir.resolve(last == -1 ? databaseName : databaseName.substring(last + 1));
        copyDatabase(databaseName, path);

        final GeoIpCache cache = new GeoIpCache(1000);
        return new DatabaseReaderLazyLoader(ProjectId.DEFAULT, cache, path, null) {
            @Override
            protected void doShutdown() throws IOException {
                if (closed != null) {
                    closed.set(true);
                }
                super.doShutdown();
            }
        };
    }

    public void testMaxmindCityWithFlexibleFieldAccessMode() throws Exception {
        String ip = "2602:306:33d3:8000::3257:9652";
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "my.target",
            getMaxmindCityLookup(),
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
        assertThat(locationArray.get(0), equalTo(-80.4572)); // longitude
        assertThat(locationArray.get(1), equalTo(25.4573)); // latitude

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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "my.target",
            getMaxmindCityLookup(),
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
        Map<String, Object> location = (Map<String, Object>) data.get("location");
        assertThat(location, notNullValue());
        assertThat(location, hasEntry("lat", 25.4573));
        assertThat(location, hasEntry("lon", -80.4572));
    }

    public void testIpinfoGeolocationWithFlexibleFieldAccessMode() throws Exception {
        String ip = "72.20.12.220";
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("ipinfo/ip_geolocation_standard_sample.mmdb"),
            () -> true,
            "my.geo",
            getIpinfoGeolocationLookup(),
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
        // Verify longitude and latitude are present in the array
        assertThat(locationArray.get(0), notNullValue()); // longitude
        assertThat(locationArray.get(1), notNullValue()); // latitude

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
            loader("ipinfo/ip_geolocation_standard_sample.mmdb"),
            () -> true,
            "my.geo",
            getIpinfoGeolocationLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "my.target",
            getMaxmindCityLookup(),
            false,
            true, // first_only = true
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
            loader("GeoLite2-ASN.mmdb"),
            () -> true,
            "ip.asn",
            getMaxmindAsnLookup(),
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
            loader("GeoLite2-Country.mmdb"),
            () -> true,
            "geo.country",
            getMaxmindCountryLookup(),
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

    public void testBothProcessorTypesWithFlexibleMode() throws Exception {
        // Test that both geoip and ip_location processors work correctly with flexible mode
        String ip = "2602:306:33d3:8000::3257:9652";

        // Test with GEOIP_TYPE
        GeoIpProcessor geoipProcessor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "ip",
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "geoip.result",
            getMaxmindCityLookup(),
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
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "ip_location.result",
            getMaxmindCityLookup(),
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

}
