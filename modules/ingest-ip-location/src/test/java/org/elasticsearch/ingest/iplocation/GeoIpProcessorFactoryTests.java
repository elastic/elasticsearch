/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.IP_LOCATION_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpProcessorFactoryTests extends ESTestCase {

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

    public void testBuildDefaults() throws Exception {
        IpDataLookup lookup = mockLookup("GeoLite2-City");
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), eq("GeoLite2-City.mmdb"), any())).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");

        Processor processor = factory.create(null, "_tag", null, config, ProjectId.DEFAULT);
        assertThat(processor, instanceOf(GeoIpProcessor.class));
        GeoIpProcessor geoIpProcessor = (GeoIpProcessor) processor;
        assertThat(geoIpProcessor.getField(), equalTo("source_field"));
        assertThat(geoIpProcessor.getTargetField(), equalTo("geoip"));
        assertThat(geoIpProcessor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(geoIpProcessor.isIgnoreMissing(), equalTo(false));
    }

    public void testBuildTargetField() throws Exception {
        IpDataLookup lookup = mockLookup("GeoLite2-City");
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("target_field", "custom_target");

        Processor processor = factory.create(null, "_tag", null, config, ProjectId.DEFAULT);
        assertThat(processor, instanceOf(GeoIpProcessor.class));
        GeoIpProcessor geoIpProcessor = (GeoIpProcessor) processor;
        assertThat(geoIpProcessor.getTargetField(), equalTo("custom_target"));
    }

    public void testBuildDbFile() throws Exception {
        IpDataLookup lookup = mockLookup("GeoLite2-Country");
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), eq("GeoLite2-Country.mmdb"), any())).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("database_file", "GeoLite2-Country.mmdb");

        Processor processor = factory.create(null, "_tag", null, config, ProjectId.DEFAULT);
        assertThat(processor, instanceOf(GeoIpProcessor.class));
        GeoIpProcessor geoIpProcessor = (GeoIpProcessor) processor;
        assertThat(geoIpProcessor.getDatabaseType(), equalTo("GeoLite2-Country"));
    }

    public void testBuildWithProperties() throws Exception {
        IpDataLookup lookup = mockLookup("GeoLite2-City");
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), eq(List.of("city_name", "country_name")))).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("properties", List.of("city_name", "country_name"));

        Processor processor = factory.create(null, "_tag", null, config, ProjectId.DEFAULT);
        assertThat(processor, instanceOf(GeoIpProcessor.class));
    }

    public void testBuildNonExistingDbFile() throws Exception {
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(null);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("database_file", "DoesNotExist.mmdb");

        Processor processor = factory.create(null, "_tag", null, config, ProjectId.DEFAULT);
        assertThat(processor, instanceOf(DatabaseUnavailableProcessor.class));
    }

    public void testBuildMissingField() throws Exception {
        IpLocationService service = mock(IpLocationService.class);
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, service);
        Map<String, Object> config = new HashMap<>();

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, "_tag", null, config, ProjectId.DEFAULT)
        );
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testBuildUnsupportedDatabase() throws Exception {
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
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Unsupported database type"));
    }

    public void testBuildIpLocationType() throws Exception {
        IpDataLookup lookup = mockLookup("IPinfo standard_asn");
        IpLocationService service = mock(IpLocationService.class);
        when(service.createIpDataLookup(anyString(), anyString(), any())).thenReturn(lookup);

        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(IP_LOCATION_TYPE, service);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("database_file", "asn.mmdb");

        Processor processor = factory.create(null, "_tag", null, config, ProjectId.DEFAULT);
        assertThat(processor, instanceOf(GeoIpProcessor.class));
        assertThat(processor.getType(), equalTo("ip_location"));
    }

    public void testDownloadDatabaseOnPipelineCreation() {
        Map<String, Object> config = new HashMap<>();
        config.put("download_database_on_pipeline_creation", false);
        assertFalse(GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(config));

        config.put("download_database_on_pipeline_creation", true);
        assertTrue(GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(config));

        assertTrue(GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(Map.of()));
    }
}
