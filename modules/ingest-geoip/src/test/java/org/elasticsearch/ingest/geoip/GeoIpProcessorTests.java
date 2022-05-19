/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GeoIpProcessorTests extends ESTestCase {

    public void testCity() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "8.8.8.8");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo("8.8.8.8"));
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(6));
        assertThat(geoData.get("ip"), equalTo("8.8.8.8"));
        assertThat(geoData.get("country_iso_code"), equalTo("US"));
        assertThat(geoData.get("country_name"), equalTo("United States"));
        assertThat(geoData.get("continent_name"), equalTo("North America"));
        assertThat(geoData.get("timezone"), equalTo("America/Chicago"));
        Map<String, Object> location = new HashMap<>();
        location.put("lat", 37.751d);
        location.put("lon", -97.822d);
        assertThat(geoData.get("location"), equalTo(location));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
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
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            true,
            false,
            "filename"
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
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

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    public void testCity_withIpV6() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        String address = "2602:306:33d3:8000::3257:9652";
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", address);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo(address));
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(9));
        assertThat(geoData.get("ip"), equalTo(address));
        assertThat(geoData.get("country_iso_code"), equalTo("US"));
        assertThat(geoData.get("country_name"), equalTo("United States"));
        assertThat(geoData.get("continent_name"), equalTo("North America"));
        assertThat(geoData.get("region_iso_code"), equalTo("US-FL"));
        assertThat(geoData.get("region_name"), equalTo("Florida"));
        assertThat(geoData.get("city_name"), equalTo("Homestead"));
        assertThat(geoData.get("timezone"), equalTo("America/New_York"));
        Map<String, Object> location = new HashMap<>();
        location.put("lat", 25.4573d);
        location.put("lon", -80.4572d);
        assertThat(geoData.get("location"), equalTo(location));
    }

    public void testCityWithMissingLocation() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "80.231.5.0");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo("80.231.5.0"));
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(1));
        assertThat(geoData.get("ip"), equalTo("80.231.5.0"));
    }

    public void testCountry() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-Country.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "82.170.213.79");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo("82.170.213.79"));
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(4));
        assertThat(geoData.get("ip"), equalTo("82.170.213.79"));
        assertThat(geoData.get("country_iso_code"), equalTo("NL"));
        assertThat(geoData.get("country_name"), equalTo("Netherlands"));
        assertThat(geoData.get("continent_name"), equalTo("Europe"));
    }

    public void testCountryWithMissingLocation() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-Country.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "80.231.5.0");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo("80.231.5.0"));
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(1));
        assertThat(geoData.get("ip"), equalTo("80.231.5.0"));
    }

    public void testAsn() throws Exception {
        String ip = "82.171.64.0";
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-ASN.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
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
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(4));
        assertThat(geoData.get("ip"), equalTo(ip));
        assertThat(geoData.get("asn"), equalTo(1136L));
        assertThat(geoData.get("organization_name"), equalTo("KPN B.V."));
        assertThat(geoData.get("network"), equalTo("82.168.0.0/14"));
    }

    public void testAddressIsNotInTheDatabase() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
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
     * Don't silently do DNS lookups or anything trappy on bogus data
     */
    public void testInvalid() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
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
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", Arrays.asList("8.8.8.8", "82.171.64.0"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> geoData = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("target_field");

        Map<String, Object> location = new HashMap<>();
        location.put("lat", 37.751d);
        location.put("lon", -97.822d);
        assertThat(geoData.get(0).get("location"), equalTo(location));

        assertThat(geoData.get(1).get("city_name"), equalTo("Hoensbroek"));
    }

    public void testListPartiallyValid() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", Arrays.asList("8.8.8.8", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> geoData = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("target_field");

        Map<String, Object> location = new HashMap<>();
        location.put("lat", 37.751d);
        location.put("lon", -97.822d);
        assertThat(geoData.get(0).get("location"), equalTo(location));

        assertThat(geoData.get(1), nullValue());
    }

    public void testListNoMatches() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            false,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", Arrays.asList("127.0.0.1", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertFalse(ingestDocument.hasField("target_field"));
    }

    public void testListFirstOnly() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", Arrays.asList("8.8.8.8", "127.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");

        Map<String, Object> location = new HashMap<>();
        location.put("lat", 37.751d);
        location.put("lon", -97.822d);
        assertThat(geoData.get("location"), equalTo(location));
    }

    public void testListFirstOnlyNoMatches() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> true,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", Arrays.asList("127.0.0.1", "127.0.0.2"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().containsKey("target_field"), is(false));
    }

    public void testInvalidDatabase() throws Exception {
        GeoIpProcessor processor = new GeoIpProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            loader("/GeoLite2-City.mmdb"),
            () -> false,
            "target_field",
            EnumSet.allOf(GeoIpProcessor.Property.class),
            false,
            true,
            "filename"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", Arrays.asList("127.0.0.1", "127.0.0.2"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().containsKey("target_field"), is(false));
        assertThat(ingestDocument.getSourceAndMetadata(), hasEntry("tags", List.of("_geoip_expired_database")));
    }

    private CheckedSupplier<DatabaseReaderLazyLoader, IOException> loader(final String path) {
        final Supplier<InputStream> databaseInputStreamSupplier = () -> GeoIpProcessor.class.getResourceAsStream(path);
        final CheckedSupplier<DatabaseReader, IOException> loader = () -> new DatabaseReader.Builder(databaseInputStreamSupplier.get())
            .build();
        final GeoIpCache cache = new GeoIpCache(1000);
        DatabaseReaderLazyLoader lazyLoader = new DatabaseReaderLazyLoader(cache, PathUtils.get(path), null, loader) {

            @Override
            long databaseFileSize() throws IOException {
                try (InputStream is = databaseInputStreamSupplier.get()) {
                    long bytesRead = 0;
                    do {
                        final byte[] bytes = new byte[1 << 10];
                        final int read = is.read(bytes);
                        if (read == -1) break;
                        bytesRead += read;
                    } while (true);
                    return bytesRead;
                }
            }

            @Override
            InputStream databaseInputStream() throws IOException {
                return databaseInputStreamSupplier.get();
            }

        };
        return () -> lazyLoader;
    }

}
