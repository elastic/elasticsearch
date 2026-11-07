/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.docvalues.Conversion;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

/** Parsing of the body of a preview or a mount: what it accepts, and the reason for each rejection. */
public class ParquetMountSpecTests extends ESTestCase {

    private static Map<String, Object> minimal() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("repository", "lake");
        body.put("paths", List.of("events/a.parquet"));
        body.put("data_stream", "events");
        return body;
    }

    private static String rejected(Map<String, Object> body) {
        return expectThrows(IllegalArgumentException.class, () -> ParquetMountSpec.parse(body, true)).getMessage();
    }

    public void testMinimalRequestGetsDefaults() {
        ParquetMountSpec spec = ParquetMountSpec.parse(minimal(), true);
        assertEquals("lake", spec.repository());
        assertEquals(List.of("events/a.parquet"), spec.paths());
        assertEquals("events", spec.dataStream());
        assertEquals(ParquetMountSpec.DEFAULT_MAX_ROWS_PER_INDEX, spec.maxRowsPerIndex());
        assertNull(spec.overrides().timestampColumn());
        assertTrue(spec.indexSettings().isEmpty());
    }

    public void testPreviewNeedsNoDataStreamButAMountDoes() {
        Map<String, Object> body = minimal();
        body.remove("data_stream");
        assertNull(ParquetMountSpec.parse(body, false).dataStream());
        assertThat(rejected(body), containsString("[data_stream] or [index] is required"));
        body.put("index", "events");
        assertEquals("events", ParquetMountSpec.parse(body, true).index());
        body.put("data_stream", "events");
        assertThat(rejected(body), containsString("not both"));
    }

    public void testOverridesExcludeAndLimitsAreRead() {
        Map<String, Object> body = minimal();
        body.put("timestamp_field", "ts");
        body.put("fields", Map.of("ts", Map.of("type", "date", "conversion", "seconds_to_millis", "name", "when")));
        body.put("exclude", List.of("params"));
        body.put("max_rows_per_index", 5_000);
        body.put("index_settings", Map.of("number_of_replicas", 0, "index.refresh_interval", "5s"));
        ParquetMountSpec spec = ParquetMountSpec.parse(body, true);

        assertEquals("ts", spec.overrides().timestampColumn());
        assertEquals(Set.of("params"), spec.overrides().exclude());
        assertEquals(
            new ParquetMappingDeriver.ColumnOverride("when", "date", Conversion.SECONDS_TO_MILLIS),
            spec.overrides().columns().get("ts")
        );
        assertEquals(5_000L, spec.maxRowsPerIndex());
        // settings without the index prefix get it
        assertEquals("0", spec.indexSettings().get("index.number_of_replicas"));
        assertEquals("5s", spec.indexSettings().get("index.refresh_interval"));
    }

    public void testMaxRowsPerIndexNeverPassesTheLimitOfAnIndex() {
        Map<String, Object> body = minimal();
        body.put("max_rows_per_index", Long.MAX_VALUE);
        assertEquals(IndexWriter.MAX_DOCS, ParquetMountSpec.parse(body, true).maxRowsPerIndex());
    }

    public void testSettingsThatTheMountDecidesAreRejected() {
        for (String setting : List.of(
            "index.parquet.repository",
            "parquet.pieces",
            "index.mode",
            "mapping.id_field.indexed",
            "number_of_shards",
            "index.hidden"
        )) {
            Map<String, Object> body = minimal();
            body.put("index_settings", Map.of(setting, "x"));
            assertThat(setting, rejected(body), containsString("is decided by the mount"));
        }
    }

    public void testRequestsThatAreNotValidAreRejectedWithAReason() {
        Map<String, Object> unknown = minimal();
        unknown.put("nope", 1);
        assertThat(rejected(unknown), containsString("unknown field [nope]"));

        Map<String, Object> noRepository = minimal();
        noRepository.remove("repository");
        assertThat(rejected(noRepository), containsString("[repository] is required"));

        for (Object paths : new Object[] { List.of(), "a.parquet", List.of(1), List.of("a.parquet", "a.parquet") }) {
            Map<String, Object> body = minimal();
            body.put("paths", paths);
            assertNotNull(rejected(body));
        }
        Map<String, Object> escape = minimal();
        escape.put("paths", List.of("a/../b.parquet"));
        assertThat(rejected(escape), containsString("below the base path"));

        for (String name : List.of("", ".hidden", "_x", "a*", "a,b")) {
            Map<String, Object> body = minimal();
            body.put("data_stream", name);
            assertNotNull(name, rejected(body));
        }

        Map<String, Object> badOverride = minimal();
        badOverride.put("fields", Map.of("ts", Map.of("typo", "date")));
        assertThat(rejected(badOverride), containsString("unknown field [fields.ts.typo]"));

        Map<String, Object> badConversion = minimal();
        badConversion.put("fields", Map.of("ts", Map.of("conversion", "hours_to_years")));
        assertNotNull(rejected(badConversion));

        for (Object max : new Object[] { 0, -5, "many" }) {
            Map<String, Object> body = minimal();
            body.put("max_rows_per_index", max);
            assertThat(rejected(body), containsString("[max_rows_per_index] must be a positive number"));
        }

        Map<String, Object> badExclude = minimal();
        badExclude.put("exclude", "params");
        assertThat(rejected(badExclude), containsString("[exclude] must be a list of column names"));
    }
}
