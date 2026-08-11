/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.validation;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.publicdata.ResultRecorder;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.DataScale;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PartitionLayout;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinInfo;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinStrategy;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCodec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataFormat;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataProvider;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.SourceVariant;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Confirms a {@link ResultRecorder} fragment round-trips through {@code CsvTestUtils#loadCsvSpecValues} --
 * the exact parser a checked-in csv-spec is read with -- for the value shapes an ES|QL response actually
 * returns: plain scalars, {@code null}, and a multi-valued (array) column.
 */
public class ResultRecorderTests extends ESTestCase {

    private static final SourceVariant VARIANT = new SourceVariant(
        "demo_v1",
        "/validation-fixtures/demo.csv-spec",
        PublicDataFormat.PARQUET,
        PublicDataCodec.SNAPPY,
        PublicDataProvider.HTTPS,
        null,
        "https://example.invalid/demo.parquet",
        "https://example.invalid/demo.parquet",
        null,
        PartitionLayout.SINGLE_FILE,
        DataScale.SMOKE,
        new PinInfo("\"abc\"", 1024, "2026-01-01T00:00:00Z", null, PinStrategy.ETAG, null),
        true,
        "test fixture"
    );

    public void testRecordedFragmentRoundTrips() throws Exception {
        List<Map<String, String>> columns = List.of(
            Map.of("name", "count", "type", "long"),
            Map.of("name", "label", "type", "keyword"),
            Map.of("name", "maybe_null", "type", "keyword"),
            Map.of("name", "tags", "type", "keyword")
        );
        List<List<Object>> values = List.of(
            Arrays.asList(42L, "hello, world", null, List.of("a", "b,c")),
            Arrays.asList(0L, "second row", "not null", List.of("solo"))
        );

        Path buildDir = createTempDir();
        ResultRecorder.record(buildDir, "demo", VARIANT, "roundtrip", columns, values);
        Path fragment = buildDir.resolve("public-data-results")
            .resolve("demo")
            .resolve(VARIANT.id())
            .resolve("roundtrip.csv-spec-fragment");
        assertTrue(Files.exists(fragment));

        String content = Files.readString(fragment, StandardCharsets.UTF_8);
        // Strip the trailing lone ";" line: loadCsvSpecValues expects only header+rows, matching how
        // CsvSpecReader hands it the accumulated `data` buffer before the closing ";" line.
        String withoutTrailer = content.substring(0, content.lastIndexOf(';'));
        CsvTestUtils.ExpectedResults parsed = CsvTestUtils.loadCsvSpecValues(withoutTrailer);

        assertEquals(List.of("count", "label", "maybe_null", "tags"), parsed.columnNames());
        assertEquals(2, parsed.values().size());
        assertEquals(42L, parsed.values().get(0).get(0));
        assertEquals("hello, world", parsed.values().get(0).get(1));
        assertNull(parsed.values().get(0).get(2));
        assertEquals(List.of("a", "b,c"), parsed.values().get(0).get(3));
        assertEquals("not null", parsed.values().get(1).get(2));
    }
}
