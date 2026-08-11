/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.validation;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalogValidator;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Exercises every {@link PublicDataCatalogValidator} rule against small synthetic catalogs, each
 * referencing the {@code /validation-fixtures/*.csv-spec} test-only fixtures (never registered as real
 * sources). {@link PublicDataCatalogOfflineValidationTests} runs the same validator against the real,
 * checked-in catalog and specs.
 */
public class PublicDataCatalogValidatorTests extends ESTestCase {

    private static final String VALID_VARIANT = """
        sources:
          - id: demo
            display_name: "Demo"
            homepage: "https://example.invalid/demo"
            license: "public domain"
            query_provenance: "test fixture"
            variants:
              - id: demo_v1
                spec: "/validation-fixtures/demo.csv-spec"
                format: PARQUET
                codec: SNAPPY
                provider: HTTPS
                resource: "https://example.invalid/demo/data.parquet"
                partition_layout: SINGLE_FILE
                scale: SMOKE
                cross_validated: true
                notes: "n/a"
                pin:
                  etag: "\\"abc\\""
                  size_bytes: 1024
                  captured_at: "2026-01-01T00:00:00Z"
        """;

    private static List<String> validate(String yaml) throws Exception {
        PublicDataCatalog catalog = PublicDataCatalog.parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
        return PublicDataCatalogValidator.validate(catalog);
    }

    public void testValidCatalogHasNoProblems() throws Exception {
        assertEquals(List.of(), validate(VALID_VARIANT));
    }

    public void testRejectsFileScheme() throws Exception {
        String yaml = VALID_VARIANT.replace(
            "resource: \"https://example.invalid/demo/data.parquet\"",
            "resource: \"file:///tmp/data.parquet\""
        );
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("file://")));
    }

    public void testRejectsUncompressedText() throws Exception {
        String yaml = VALID_VARIANT.replace("format: PARQUET", "format: CSV").replace("codec: SNAPPY", "codec: UNCOMPRESSED");
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("UNCOMPRESSED")));
    }

    public void testRejectsSnappyText() throws Exception {
        String yaml = VALID_VARIANT.replace("format: PARQUET", "format: TSV");
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("SNAPPY")));
    }

    public void testRejectsMultiFileWithoutObjectCount() throws Exception {
        String yaml = """
            sources:
              - id: demo
                display_name: "Demo"
                homepage: "https://example.invalid/demo"
                license: "public domain"
                query_provenance: "test fixture"
                variants:
                  - id: demo_v1
                    spec: "/validation-fixtures/demo.csv-spec"
                    format: PARQUET
                    codec: SNAPPY
                    provider: HTTPS
                    resource: "https://example.invalid/demo/*.parquet"
                    partition_layout: UNIFORM_SHARDS
                    scale: SMOKE
                    cross_validated: true
                    notes: "n/a"
                    pin:
                      etag: "\\"abc\\""
                      size_bytes: 1024
                      captured_at: "2026-01-01T00:00:00Z"
            """;
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("object_count")));
    }

    public void testRejectsSingleFileWithGlob() throws Exception {
        String yaml = VALID_VARIANT.replace(
            "resource: \"https://example.invalid/demo/data.parquet\"",
            "resource: \"https://example.invalid/demo/*.parquet\""
        );
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("glob")));
    }

    public void testRejectsNonPositiveSize() throws Exception {
        String yaml = VALID_VARIANT.replace("size_bytes: 1024", "size_bytes: 0");
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("size_bytes")));
    }

    public void testRejectsDuplicateResourceAcrossVariants() throws Exception {
        String secondVariant = """
                  - id: demo_v2
                    spec: "/validation-fixtures/demo.csv-spec"
                    format: PARQUET
                    codec: ZSTD
                    provider: HTTPS
                    resource: "https://example.invalid/demo/data.parquet"
                    partition_layout: SINGLE_FILE
                    scale: SMOKE
                    cross_validated: true
                    notes: "n/a"
                    pin:
                      etag: "\\"abc\\""
                      size_bytes: 1024
                      captured_at: "2026-01-01T00:00:00Z"
            """;
        String yaml = VALID_VARIANT + secondVariant;
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("Duplicate resource")));
    }

    public void testRejectsMismatchedDatasetTemplate() throws Exception {
        String yaml = VALID_VARIANT.replace("/validation-fixtures/demo.csv-spec", "/validation-fixtures/mismatched-template.csv-spec");
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("must be exactly")));
    }

    public void testRejectsOversizedResultSet() throws Exception {
        String yaml = VALID_VARIANT.replace("/validation-fixtures/demo.csv-spec", "/validation-fixtures/oversized.csv-spec");
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("exceeding the absolute maximum")));
    }

    public void testAcceptsContentSignatureStrategyWithSignature() throws Exception {
        // VALID_VARIANT's text block strips down to 10-space-indented pin.* keys (siblings of
        // captured_at), not the source file's own indentation.
        String yaml = VALID_VARIANT.replace(
            "captured_at: \"2026-01-01T00:00:00Z\"",
            "captured_at: \"2026-01-01T00:00:00Z\"\n          strategy: CONTENT_SIGNATURE\n" + "          content_signature: \"rows=42\""
        );
        assertEquals(List.of(), validate(yaml));
    }

    public void testRejectsMissingSpecResource() throws Exception {
        String yaml = VALID_VARIANT.replace("/validation-fixtures/demo.csv-spec", "/validation-fixtures/does-not-exist.csv-spec");
        List<String> problems = validate(yaml);
        assertTrue(problems.toString(), problems.stream().anyMatch(p -> p.contains("not found")));
    }
}
