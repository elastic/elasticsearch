/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.validation;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PartitionLayout;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinInfo;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinStrategy;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCodec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataFormat;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataProvider;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.SourceVariant;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link PublicDataCatalog#parse} against small synthetic YAML documents, independent of
 * the real checked-in {@code public-data-catalog.yml} so they keep exercising every parsing rule even as
 * that catalog grows. {@link PublicDataCatalogOfflineValidationTests} covers the real catalog+specs.
 */
public class PublicDataCatalogParsingTests extends ESTestCase {

    private static final String VALID_YAML = """
        sources:
          - id: demo
            display_name: "Demo source"
            homepage: "https://example.invalid/demo"
            license: "public domain"
            query_provenance: "hand-written for this test"
            variants:
              - id: demo_parquet_single
                spec: "/specs/demo.csv-spec"
                format: PARQUET
                codec: SNAPPY
                provider: HTTPS
                resource: "https://example.invalid/demo/data.parquet"
                partition_layout: SINGLE_FILE
                scale: SMOKE
                cross_validated: true
                notes: "synthetic"
                pin:
                  etag: "\\"abc123\\""
                  size_bytes: 1024
                  captured_at: "2026-01-01T00:00:00Z"
        """;

    public void testParsesAValidCatalog() throws Exception {
        PublicDataCatalog catalog = PublicDataCatalog.parse(new ByteArrayInputStream(VALID_YAML.getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, catalog.sources().size());
        var demo = catalog.requireSourceId("demo");
        assertEquals("Demo source", demo.displayName());
        assertEquals(1, demo.variants().size());

        SourceVariant variant = demo.variants().get(0);
        assertEquals("/specs/demo.csv-spec", variant.specResource());
        assertEquals(PublicDataFormat.PARQUET, variant.format());
        assertEquals(PublicDataCodec.SNAPPY, variant.codec());
        assertEquals(PublicDataProvider.HTTPS, variant.provider());
        assertEquals(PartitionLayout.SINGLE_FILE, variant.partitionLayout());
        assertEquals("https://example.invalid/demo/data.parquet", variant.resource());
        // pin_check_uri was omitted, so it must default to resource().
        assertEquals(variant.resource(), variant.pinCheckUri());
        assertEquals("\"abc123\"", variant.pin().etag());
        assertEquals(1024L, variant.pin().sizeBytes());
        assertTrue(variant.crossValidated());
        // strategy was omitted, so it must default to ETAG.
        assertEquals(PinStrategy.ETAG, variant.pin().strategy());
        assertNull(variant.pin().contentSignature());
    }

    public void testRejectsDuplicateSourceIds() {
        String yaml = VALID_YAML + VALID_YAML.replace("sources:\n", "");
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)))
        );
        assertTrue(e.getMessage().contains("Duplicate"));
    }

    public void testRejectsMissingRequiredField() {
        // Removes the whole homepage line, including its leading indentation, so the remaining lines'
        // indentation (and hence the YAML mapping structure) is untouched.
        String yaml = VALID_YAML.replace("    homepage: \"https://example.invalid/demo\"\n", "");
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)))
        );
        assertTrue(e.getMessage().contains("homepage"));
    }

    public void testRejectsUnknownFormat() {
        String yaml = VALID_YAML.replace("format: PARQUET", "format: AVRO");
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)))
        );
        assertTrue(e.getMessage().contains("AVRO"));
    }

    public void testRejectsSourceWithNoVariants() {
        String yaml = """
            sources:
              - id: empty
                display_name: "Empty"
                homepage: "https://example.invalid/empty"
                license: "public domain"
                query_provenance: "n/a"
                variants: []
            """;
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)))
        );
        assertTrue(e.getMessage().contains("no variants"));
    }

    public void testParsesContentSignatureStrategy() throws Exception {
        // VALID_YAML's text block strips down to 10-space-indented pin.* keys (siblings of captured_at),
        // not the source file's own indentation -- see the stripped layout asserted by testParsesAValidCatalog.
        String yaml = VALID_YAML.replace(
            "captured_at: \"2026-01-01T00:00:00Z\"",
            "captured_at: \"2026-01-01T00:00:00Z\"\n          strategy: CONTENT_SIGNATURE\n"
                + "          content_signature: \"rows=4617295;stations=6920\""
        );
        PublicDataCatalog catalog = PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
        PinInfo pin = catalog.requireSourceId("demo").variants().get(0).pin();
        assertEquals(PinStrategy.CONTENT_SIGNATURE, pin.strategy());
        assertEquals("rows=4617295;stations=6920", pin.contentSignature());
    }

    public void testRejectsContentSignatureStrategyWithoutSignature() {
        String yaml = VALID_YAML.replace(
            "captured_at: \"2026-01-01T00:00:00Z\"",
            "captured_at: \"2026-01-01T00:00:00Z\"\n          strategy: CONTENT_SIGNATURE"
        );
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)))
        );
        assertTrue(e.getMessage().contains("content_signature"));
    }

    public void testSettingsMapIsSerializedToJson() throws Exception {
        String yaml = VALID_YAML.replace("notes: \"synthetic\"", "notes: \"synthetic\"\n        settings:\n          header_row: false");
        PublicDataCatalog catalog = PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
        String settingsJson = catalog.requireSourceId("demo").variants().get(0).settingsJson();
        assertNotNull(settingsJson);
        assertTrue(settingsJson.contains("\"header_row\":false"));
    }
}
