/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.CheckedSupplier;
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
import static org.elasticsearch.ingest.geoip.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoIpProcessorTests extends ESTestCase {

    private static IpDataLookup ipDataLookupAll(final Database database) {
        return IpDataLookupFactories.getMaxmindLookup(database).apply(database.properties());
    }

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

    public void testNullValueWithIgnoreMissing() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "82.171.64.0"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> geoData = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData, notNullValue());
        assertThat(geoData.size(), equalTo(2));
        assertThat(geoData.get(0).get("location"), equalTo(Map.of("lat", 37.751d, "lon", -97.822d)));
        assertThat(geoData.get(1).get("city_name"), equalTo("Hoensbroek"));
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
            ipDataLookupAll(Database.City),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> geoData = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData, notNullValue());
        assertThat(geoData.size(), equalTo(2));
        assertThat(geoData.get(0).get("location"), equalTo(Map.of("lat", 37.751d, "lon", -97.822d)));
        assertThat(geoData.get(1), nullValue());
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
            ipDataLookupAll(Database.City),
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
        }, () -> true, "target_field", ipDataLookupAll(Database.City), false, false, "filename");

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "82.171.64.0"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> geoData = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData, notNullValue());
        assertThat(geoData.size(), equalTo(2));
        assertThat(geoData.get(0).get("location"), equalTo(Map.of("lat", 37.751d, "lon", -97.822d)));
        assertThat(geoData.get(1).get("city_name"), equalTo("Hoensbroek"));

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
            ipDataLookupAll(Database.City),
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("8.8.8.8", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData, notNullValue());
        assertThat(geoData.get("location"), equalTo(Map.of("lat", 37.751d, "lon", -97.822d)));
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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
            ipDataLookupAll(Database.City),
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

    private CheckedSupplier<IpDatabase, IOException> loader(final String path) {
        var loader = loader(path, null);
        return () -> loader;
    }

    private DatabaseReaderLazyLoader loader(final String databaseName, final AtomicBoolean closed) {
        Path path = tmpDir.resolve(databaseName);
        copyDatabase(databaseName, path);

        final GeoIpCache cache = new GeoIpCache(1000);
        return new DatabaseReaderLazyLoader(cache, path, null) {
            @Override
            protected void doShutdown() throws IOException {
                if (closed != null) {
                    closed.set(true);
                }
                super.doShutdown();
            }
        };
    }

}
