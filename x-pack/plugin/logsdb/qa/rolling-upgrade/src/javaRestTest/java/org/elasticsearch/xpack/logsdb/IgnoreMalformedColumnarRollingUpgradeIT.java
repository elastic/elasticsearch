/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * BWC test that verifies malformed values in strict-columnar (logsdb_columnar / columnar) indices
 * survive a rolling upgrade with their synthetic source intact.
 *
 * <p>Covers multiple field types that went through the ignore_malformed-to-._on_failure
 * routing change: {@code long}, {@code date}, {@code ip}, {@code boolean}, and {@code scaled_float}.
 */
public class IgnoreMalformedColumnarRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    private static final String INDEX_NAME = "bwc-ignore-malformed-columnar";

    /**
     * The {@code columnar} index mode (and its logsdb-flavoured sibling {@code logsdb_columnar})
     * were introduced as tech preview in 9.5.0. Strict-columnar behaviour — and therefore this
     * BWC scenario — does not apply to older clusters.
     */
    private static final String COLUMNAR_MODES_MIN_VERSION = "9.5.0";

    /**
     * Each record describes a field in the test index: the Elasticsearch field name, mapping type,
     * a well-formed JSON value to index (used in docs 1 and 3), the expected synthetic-source
     * representation of that value (may differ after normalization, e.g. for scaled_float), and
     * a malformed JSON token/value string that the mapper cannot parse (used in doc 2).
     *
     * <p>Notes on expected values:
     * <ul>
     *   <li>{@code date} — format is left as the default ({@code strict_date_optional_time}), so the
     *       synthetic source re-emits the input string unchanged.</li>
     *   <li>{@code scaled_float} — 1.5 with scaling_factor=100 encodes as 150 → decodes back to 1.5
     *       exactly, so the expected value equals the input.</li>
     *   <li>Malformed values always round-trip as the raw JSON string because the value is captured
     *       verbatim by {@code XContentDataHelper}.</li>
     * </ul>
     */
    private record FieldCase(
        String name,
        String type,
        String mappingExtras,
        Object goodValue,
        Object expectedGoodValue,
        String malformedValue
    ) {}

    private static final List<FieldCase> FIELDS = List.of(
        new FieldCase("long_value", "long", "", 42, 42, "\"not-a-number\""),
        new FieldCase("date_value", "date", "", "\"2026-01-02T03:04:05.000Z\"", "2026-01-02T03:04:05.000Z", "\"not-a-date\""),
        new FieldCase("ip_value", "ip", "", "\"192.168.1.1\"", "192.168.1.1", "\"not-an-ip\""),
        new FieldCase("boolean_value", "boolean", "", true, true, "\"not-a-boolean\""),
        new FieldCase("float_value", "scaled_float", ", \"scaling_factor\": 100", 1.5, 1.5, "\"not-a-float\"")
    );

    public void testIgnoreMalformedSurvivesUpgrade() throws Exception {
        var version = System.getProperty("tests.old_cluster_version") != null
            ? org.elasticsearch.test.cluster.util.Version.fromString(System.getProperty("tests.old_cluster_version"))
            : org.elasticsearch.test.cluster.util.Version.CURRENT;

        LuceneTestCase.assumeTrue(
            "columnar index modes require old cluster >= " + COLUMNAR_MODES_MIN_VERSION,
            version.onOrAfter(org.elasticsearch.test.cluster.util.Version.fromString(COLUMNAR_MODES_MIN_VERSION))
        );

        // Build the mapping from all field cases.
        var properties = new StringBuilder();
        for (FieldCase field : FIELDS) {
            if (properties.length() > 0) {
                properties.append(",\n");
            }
            properties.append("""
                "%s": {
                  "type": "%s"%s,
                  "ignore_malformed": true
                }""".formatted(field.name(), field.type(), field.mappingExtras()));
        }
        String mapping = """
            {
              "properties": {
                %s
              }
            }""".formatted(properties);

        // replica count of 1 ensures the shard remains accessible during rolling upgrade while
        // one node at a time is restarted (0 replicas would make the shard unavailable on restart).
        createIndex(INDEX_NAME, Settings.builder().put("index.mode", "columnar").put("index.number_of_replicas", 1).build(), mapping);

        // Doc 1: well-formed values for every field.
        indexDocument("1", buildGoodDoc());

        // Doc 2: malformed values for every field. Old code stores each value in the per-field
        // ._ignore_malformed binary doc-values column. New code must still read that column for
        // indices whose created-version pre-dates the merge into ._on_failure.
        indexDocument("2", buildMalformedDoc());

        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Verify source before the upgrade: both docs must be present with all field values intact.
        verifyAllFields("1", /* malformed */ false);
        verifyAllFields("2", /* malformed */ true);

        clusterRollingUpgrade(nodeIndex -> {
            ensureGreen(INDEX_NAME);
            // After each node is upgraded, re-verify that the sources are intact.
            // Without the index-version guard in addFallbackLayers(), malformed fields in doc 2
            // will disappear on the upgraded node because the new read path looks in ._on_failure
            // but the values were written to ._ignore_malformed by the old code.
            verifyAllFields("1", /* malformed */ false);
            verifyAllFields("2", /* malformed */ true);
        });

        // After the full upgrade, verify a newly-written malformed doc round-trips correctly.
        // With the fix, the old index's created-version gates the write path too.
        indexDocument("3", buildMalformedDoc());
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        verifyAllFields("3", /* malformed */ true);
    }

    private String buildGoodDoc() {
        var sb = new StringBuilder("{");
        for (int i = 0; i < FIELDS.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(FIELDS.get(i).name()).append("\": ").append(FIELDS.get(i).goodValue());
        }
        sb.append("}");
        return sb.toString();
    }

    private String buildMalformedDoc() {
        var sb = new StringBuilder("{");
        for (int i = 0; i < FIELDS.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(FIELDS.get(i).name()).append("\": ").append(FIELDS.get(i).malformedValue());
        }
        sb.append("}");
        return sb.toString();
    }

    private void indexDocument(String id, String body) throws IOException {
        var request = new Request("PUT", "/" + INDEX_NAME + "/_doc/" + id);
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    /**
     * Fetches a single document by id and asserts that every field in {@link #FIELDS} has
     * the expected synthetic-source value.
     */
    private void verifyAllFields(String id, boolean malformed) throws IOException {
        var searchRequest = new Request("GET", "/" + INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {"ids": {"values": ["%s"]}},
              "size": 1
            }
            """.formatted(id));
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var body = entityAsMap(response);

        assertThat(
            "search for doc [" + id + "] should return 1 hit (got _shards.failed=" + ObjectPath.evaluate(body, "_shards.failed") + ")",
            ObjectPath.evaluate(body, "hits.total.value"),
            equalTo(1)
        );

        Map<?, ?> source = ObjectPath.evaluate(body, "hits.hits.0._source");
        for (FieldCase field : FIELDS) {
            Object expected = malformed
                // Malformed values are captured verbatim (without surrounding quotes) by XContentDataHelper.
                ? field.malformedValue().replaceAll("^\"(.*)\"$", "$1")
                : field.expectedGoodValue();
            assertThat(
                "doc [" + id + "] field [" + field.name() + "] must be present in synthetic source",
                source.get(field.name()),
                equalTo(expected)
            );
        }
    }
}
