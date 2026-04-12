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

import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.CLASSIC;
import static org.elasticsearch.ingest.IngestPipelineFieldAccessPattern.FLEXIBLE;
import static org.elasticsearch.ingest.IngestPipelineTestUtils.runWithAccessPattern;
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

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
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

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    public void testDatabaseUnavailableWithIgnoreMissing() throws Exception {
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
            true,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "8.8.8.8");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().get("geoip"), nullValue());
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("tags"), is(false));
    }

    public void testExceptionPropagates() throws Exception {
        IpDataLookupInfo info = mockInfo("GeoLite2-City");
        IpDataLookup lookup = mock(IpDataLookup.class);
        when(lookup.isValid()).thenReturn(true);
        when(lookup.getInfo()).thenReturn(info);
        when(lookup.lookup(anyString())).thenThrow(new IOException("test exception"));

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
        IOException e = expectThrows(IOException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("test exception"));
    }

    public void testListNoMatches() throws Exception {
        IpDataLookup lookup = mockLookup(Map.of());
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
        document.put("source_field", List.of("10.0.0.1", "10.0.0.2"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().containsKey("geoip"), is(false));
    }

    public void testListFirstOnlyNoMatches() throws Exception {
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
        document.put("source_field", List.of("10.0.0.1", "10.0.0.2"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().containsKey("geoip"), is(false));
    }

    public void testListPartiallyValid() throws Exception {
        Map<String, Object> data1 = Map.of("city_name", "Seattle", "location", Map.of("lat", 47.6062, "lon", -122.3321));
        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", data1));

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
        document.put("source_field", List.of("1.2.3.4", "10.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("geoip");
        assertThat(data, notNullValue());
        assertThat(data.size(), equalTo(2));
        assertThat(data.get(0).get("city_name"), equalTo("Seattle"));
        assertThat(data.get(1), nullValue());
    }

    public void testFlexibleFieldAccessMode() throws Exception {
        Map<String, Object> cityData = new HashMap<>();
        cityData.put("ip", "1.2.3.4");
        cityData.put("city_name", "Seattle");
        cityData.put("country_name", "United States");
        cityData.put("location", Map.of("lat", 47.6062, "lon", -122.3321));

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", cityData));
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "my.target",
            false,
            false,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "1.2.3.4");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        assertThat(sourceAndMetadata.get("my.target.ip"), equalTo("1.2.3.4"));
        assertThat(sourceAndMetadata.containsKey("my.target.city_name"), is(true));
        assertThat(sourceAndMetadata.get("my.target.city_name"), equalTo("Seattle"));
        assertThat(sourceAndMetadata.containsKey("my.target.country_name"), is(true));
        assertThat(sourceAndMetadata.get("my.target.country_name"), equalTo("United States"));

        assertThat(sourceAndMetadata.containsKey("my.target.location"), is(true));
        Object location = sourceAndMetadata.get("my.target.location");
        assertThat(location, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Double> locationArray = (List<Double>) location;
        assertThat(locationArray.size(), equalTo(2));
        assertThat(locationArray.get(0), equalTo(-122.3321));
        assertThat(locationArray.get(1), equalTo(47.6062));

        assertThat(sourceAndMetadata.containsKey("my.target"), is(false));
    }

    public void testClassicFieldAccessMode() throws Exception {
        Map<String, Object> cityData = new HashMap<>();
        cityData.put("ip", "1.2.3.4");
        cityData.put("city_name", "Seattle");
        cityData.put("country_name", "United States");
        cityData.put("location", Map.of("lat", 47.6062, "lon", -122.3321));

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", cityData));
        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "my.target",
            false,
            false,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "1.2.3.4");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        ingestDocument = runWithAccessPattern(CLASSIC, ingestDocument, processor);

        assertThat(ingestDocument.hasField("my.target"), is(true));
        Object target = ingestDocument.getFieldValue("my.target", Object.class);
        assertThat(target, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) target;
        assertThat(data, notNullValue());
        assertThat(data.get("ip"), equalTo("1.2.3.4"));
        assertThat(data.get("city_name"), equalTo("Seattle"));

        @SuppressWarnings("unchecked")
        Map<String, Object> locationMap = (Map<String, Object>) data.get("location");
        assertThat(locationMap, notNullValue());
        assertThat(locationMap, hasEntry("lat", 47.6062));
        assertThat(locationMap, hasEntry("lon", -122.3321));
    }

    public void testFlexibleFieldAccessModeNoLocation() throws Exception {
        Map<String, Object> asnData = new HashMap<>();
        asnData.put("ip", "1.2.3.4");
        asnData.put("asn", 15169);
        asnData.put("organization_name", "Google LLC");
        asnData.put("network", "1.2.3.0/24");

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", asnData));
        GeoIpProcessor processor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "ip.asn",
            false,
            false,
            "GeoLite2-ASN.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "1.2.3.4");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("ip.asn.ip"), is(true));
        assertThat(sourceAndMetadata.get("ip.asn.ip"), equalTo("1.2.3.4"));
        assertThat(sourceAndMetadata.containsKey("ip.asn.asn"), is(true));
        assertThat(sourceAndMetadata.get("ip.asn.asn"), equalTo(15169));
        assertThat(sourceAndMetadata.containsKey("ip.asn.organization_name"), is(true));
        assertThat(sourceAndMetadata.containsKey("ip.asn.network"), is(true));

        assertThat(sourceAndMetadata.containsKey("ip.asn"), is(false));
    }

    public void testBothProcessorTypesWithFlexibleMode() throws Exception {
        Map<String, Object> cityData = new HashMap<>();
        cityData.put("city_name", "Seattle");
        cityData.put("location", Map.of("lat", 47.6062, "lon", -122.3321));

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", cityData));

        GeoIpProcessor geoipProcessor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "ip",
            lookup,
            "geoip.result",
            false,
            false,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("ip", "1.2.3.4");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, geoipProcessor);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("geoip.result.city_name"), is(true));
        assertThat(sourceAndMetadata.get("geoip.result.city_name"), equalTo("Seattle"));
        assertThat(sourceAndMetadata.containsKey("geoip.result.location"), is(true));
        assertThat(sourceAndMetadata.containsKey("geoip.result"), is(false));

        GeoIpProcessor ipLocationProcessor = new GeoIpProcessor(
            IP_LOCATION_TYPE,
            randomAlphaOfLength(10),
            null,
            "ip",
            mockLookup(Map.of("1.2.3.4", cityData)),
            "ip_location.result",
            false,
            false,
            "GeoLite2-City.mmdb"
        );

        document = new HashMap<>();
        document.put("ip", "1.2.3.4");
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, ipLocationProcessor);

        sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("ip_location.result.city_name"), is(true));
        assertThat(sourceAndMetadata.get("ip_location.result.city_name"), equalTo("Seattle"));
        assertThat(sourceAndMetadata.containsKey("ip_location.result.location"), is(true));
        assertThat(sourceAndMetadata.containsKey("ip_location.result"), is(false));
    }

    public void testListWithFlexibleFieldAccessMode() throws Exception {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("ip", "1.2.3.4");
        data1.put("city_name", "Seattle");
        data1.put("location", Map.of("lat", 47.6062, "lon", -122.3321));

        Map<String, Object> data2 = new HashMap<>();
        data2.put("ip", "5.6.7.8");
        data2.put("city_name", "Portland");
        data2.put("location", Map.of("lat", 45.5152, "lon", -122.6784));

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", data1, "5.6.7.8", data2));

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "my.target",
            false,
            false,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("1.2.3.4", "5.6.7.8"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();

        assertThat(sourceAndMetadata.containsKey("my.target.location"), is(true));
        Object location = sourceAndMetadata.get("my.target.location");
        assertThat(location, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<List<Double>> locationList = (List<List<Double>>) location;
        assertThat(locationList.size(), equalTo(2));
        assertThat(locationList.get(0).get(0), equalTo(-122.3321));
        assertThat(locationList.get(0).get(1), equalTo(47.6062));
        assertThat(locationList.get(1).get(0), equalTo(-122.6784));
        assertThat(locationList.get(1).get(1), equalTo(45.5152));

        assertThat(sourceAndMetadata.containsKey("my.target.city_name"), is(true));
        Object cityName = sourceAndMetadata.get("my.target.city_name");
        assertThat(cityName, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> cityNameList = (List<String>) cityName;
        assertThat(cityNameList.size(), equalTo(2));
        assertThat(cityNameList.get(0), equalTo("Seattle"));
        assertThat(cityNameList.get(1), equalTo("Portland"));

        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        Object ip = sourceAndMetadata.get("my.target.ip");
        assertThat(ip, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> ipList = (List<String>) ip;
        assertThat(ipList.size(), equalTo(2));
        assertThat(ipList.get(0), equalTo("1.2.3.4"));
        assertThat(ipList.get(1), equalTo("5.6.7.8"));

        assertThat(sourceAndMetadata.containsKey("my.target"), is(false));
    }

    public void testListPartiallyValidWithFlexibleFieldAccessMode() throws Exception {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("ip", "1.2.3.4");
        data1.put("city_name", "Seattle");
        data1.put("location", Map.of("lat", 47.6062, "lon", -122.3321));

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", data1));

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "my.target",
            false,
            false,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("1.2.3.4", "10.0.0.1"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();

        assertThat(sourceAndMetadata.containsKey("my.target.location"), is(true));
        Object location = sourceAndMetadata.get("my.target.location");
        assertThat(location, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> locationList = (List<Object>) location;
        assertThat(locationList.size(), equalTo(2));
        assertThat(locationList.get(0), instanceOf(List.class));
        assertThat(locationList.get(1), nullValue());

        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        Object ip = sourceAndMetadata.get("my.target.ip");
        assertThat(ip, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> ipList = (List<String>) ip;
        assertThat(ipList.size(), equalTo(2));
        assertThat(ipList.get(0), equalTo("1.2.3.4"));
        assertThat(ipList.get(1), nullValue());
    }

    public void testArrayWithFlexibleFieldAccessModeFirstOnly() throws Exception {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("ip", "1.2.3.4");
        data1.put("city_name", "Seattle");

        Map<String, Object> data2 = new HashMap<>();
        data2.put("ip", "5.6.7.8");
        data2.put("city_name", "Portland");

        IpDataLookup lookup = mockLookup(Map.of("1.2.3.4", data1, "5.6.7.8", data2));

        GeoIpProcessor processor = new GeoIpProcessor(
            GEOIP_TYPE,
            randomAlphaOfLength(10),
            null,
            "source_field",
            lookup,
            "my.target",
            false,
            true,
            "GeoLite2-City.mmdb"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", List.of("1.2.3.4", "5.6.7.8"));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        ingestDocument = runWithAccessPattern(FLEXIBLE, ingestDocument, processor);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.containsKey("my.target.ip"), is(true));
        assertThat(sourceAndMetadata.get("my.target.ip"), equalTo("1.2.3.4"));
        assertThat(sourceAndMetadata.containsKey("my.target.city_name"), is(true));
        assertThat(sourceAndMetadata.get("my.target.city_name"), equalTo("Seattle"));
    }
}
