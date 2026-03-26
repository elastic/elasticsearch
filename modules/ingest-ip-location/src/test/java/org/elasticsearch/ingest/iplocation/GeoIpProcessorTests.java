/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.IP_LOCATION_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpProcessorTests extends ESTestCase {

    private static IpDataLookupInfo mockInfo(String databaseType) {
        IpDataLookupInfo info = mock(IpDataLookupInfo.class);
        when(info.getDatabaseType()).thenReturn(databaseType);
        SequencedMap<String, Class<?>> fields = new LinkedHashMap<>();
        fields.put("city_name", String.class);
        fields.put("country_name", String.class);
        when(info.getFields()).thenReturn(fields);
        return info;
    }

    private static IpDataLookup mockLookup(Map<String, Map<String, Object>> responses) throws IOException {
        IpDataLookupInfo info = mockInfo("GeoLite2-City");
        IpDataLookup lookup = mock(IpDataLookup.class);
        when(lookup.isValid()).thenReturn(true);
        when(lookup.getInfo()).thenReturn(info);
        when(lookup.lookup(anyString())).thenAnswer(invocation -> {
            String ip = invocation.getArgument(0);
            Map<String, Object> result = responses.get(ip);
            if (result == null) {
                return Map.of();
            }
            return result;
        });
        return lookup;
    }

    public void testSingleIp() throws Exception {
        Map<String, Object> cityData = new HashMap<>();
        cityData.put("city_name", "Seattle");
        cityData.put("country_name", "United States");

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", cityData));
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "1.2.3.4");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("geoip");
        assertThat(geoData, notNullValue());
        assertThat(geoData, hasEntry("city_name", "Seattle"));
        assertThat(geoData, hasEntry("country_name", "United States"));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        IpDataLookup lookup = mockLookup(Map.of());
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            true,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().get("geoip"), nullValue());
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        IpDataLookup lookup = mockLookup(Map.of());
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("field [source_field] is null"));
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        IpDataLookup lookup = mockLookup(Map.of());
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            true,
            true,
            "GeoLite2-City.mmdb"
        );

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().get("geoip"), nullValue());
    }

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
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "1.2.3.4");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<String> tags = (List<String>) ingestDocument.getSourceAndMetadata().get("tags");
        assertThat(tags, notNullValue());
        assertTrue(tags.contains("_geoip_expired_database"));
    }

    public void testDatabaseUnavailable() throws Exception {
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
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "10.0.0.1");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<String> tags = (List<String>) ingestDocument.getSourceAndMetadata().get("tags");
        assertThat(tags, notNullValue());
        assertTrue(tags.contains("_geoip_database_unavailable_GeoLite2-City.mmdb"));
    }

    public void testIpNotFound() throws Exception {
        IpDataLookup lookup = mockLookup(Map.of());
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "10.0.0.1");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("geoip"), nullValue());
    }

    public void testListAllValid() throws Exception {
        Map<String, Object> data1 = Map.of("city_name", "Seattle");
        Map<String, Object> data2 = Map.of("city_name", "Portland");
        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", data1, "5.6.7.8", data2));

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            false,
            false,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("1.2.3.4", "5.6.7.8"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> geoDataList = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("geoip");
        assertThat(geoDataList, notNullValue());
        assertThat(geoDataList.size(), equalTo(2));
        assertThat(geoDataList.get(0), hasEntry("city_name", "Seattle"));
        assertThat(geoDataList.get(1), hasEntry("city_name", "Portland"));
    }

    public void testListFirstOnly() throws Exception {
        Map<String, Object> data1 = Map.of("city_name", "Seattle");
        Map<String, Object> data2 = Map.of("city_name", "Portland");
        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", data1, "5.6.7.8", data2));

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("1.2.3.4", "5.6.7.8"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("geoip");
        assertThat(geoData, notNullValue());
        assertThat(geoData, hasEntry("city_name", "Seattle"));
    }

    public void testInvalidFieldType() throws Exception {
        IpDataLookup lookup = mockLookup(Map.of());
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
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
        IpDataLookup lookup = mockLookup(Map.of());
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "geoip",
            false,
            true,
            "GeoLite2-City.mmdb"
        );
        assertThat(processor.getType(), equalTo("geoip"));
    }

    public void testTypeIpLocation() throws Exception {
        IpDataLookup lookup = mockLookup(Map.of());
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "ip_location",
            false,
            true,
            "GeoLite2-City.mmdb"
        );
        assertThat(processor.getType(), equalTo("ip_location"));
    }
}
