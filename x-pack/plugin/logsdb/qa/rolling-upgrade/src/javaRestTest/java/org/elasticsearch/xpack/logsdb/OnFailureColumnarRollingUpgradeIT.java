/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperFeatures;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Rolling-upgrade BWC test for {@code doc_values.on_failure=ignore} in {@code logsdb_columnar} index mode.
 *
 * <p>Covers two violation paths that take different code routes:
 * <ol>
 *   <li>{@code multi_value:false, on_failure:ignore} — extra values are stored in a sidecar binary
 *       doc-values column ({@code <field>._on_failure}) written via
 *       {@code OnFailureStoredValues.storeEncoded}. The encoding is keyed on the index's created version,
 *       so old-written sidecar bytes must stay readable by upgraded nodes.</li>
 *   <li>{@code nullability:false, on_failure:ignore} — missing/null/empty-array is accepted by marking
 *       the field in {@code _ignored}; no sidecar value is written.</li>
 * </ol>
 */
public class OnFailureColumnarRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    private static final String INDEX_NAME = "bwc-on-failure-columnar";

    private static final String MAPPING = """
        {
          "properties": {
            "single_kw": {
              "type": "keyword",
              "doc_values": { "multi_value": false, "on_failure": "ignore" }
            },
            "required_kw": {
              "type": "keyword",
              "doc_values": { "nullability": false, "on_failure": "ignore" }
            }
          }
        }
        """;

    /**
     * Captures a single indexed document together with the field values expected after round-trip through synthetic source.
     *
     * @param id               document id
     * @param body             JSON body sent to the index API
     * @param expectedSingleKw expected {@code _source.single_kw}; {@code null} means field absent from source
     * @param expectedRequiredKw expected {@code _source.required_kw}; {@code null} means field absent from source
     * @param expectedIgnored  expected {@code _ignored} stored-field values; empty means the field is absent entirely
     * @param searchablePrimary the value that must be findable via a term query on {@code single_kw}
     * @param sidecarValues    values stored in the sidecar that must NOT be findable via a term query
     */
    private record ExpectedDoc(
        String id,
        String body,
        Object expectedSingleKw,
        Object expectedRequiredKw,
        List<String> expectedIgnored,
        String searchablePrimary,
        List<String> sidecarValues
    ) {}

    @Override
    public String getEnsureGreenTimeout() {
        return "2m";
    }

    public void testOnFailureSurvivesUpgrade() throws Exception {
        assumeTrue(
            "doc_values.on_failure requires old cluster feature [" + MapperFeatures.DOC_VALUES_ON_FAILURE.id() + "]",
            oldClusterHasFeature(MapperFeatures.DOC_VALUES_ON_FAILURE)
        );

        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
            // ._on_failure sidecar is surfaced only through synthetic source; columnar_stored prunes it and would silently gut this test.
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic")
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        createIndex(INDEX_NAME, settings, MAPPING);

        String ts = formatInstant(Instant.parse("2024-01-01T00:00:00Z"));

        List<ExpectedDoc> expectedDocs = new ArrayList<>();

        // doc 1: multi-value violation — first value kept, rest go to sidecar
        expectedDocs.add(
            new ExpectedDoc(
                "1",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":[\"val1\",\"val2\",\"val3\"],\"required_kw\":\"present\"}",
                List.of("val1", "val2", "val3"),
                "present",
                List.of("single_kw"),
                "val1",
                List.of("val2", "val3")
            )
        );

        // doc 2: no violation — negative control
        expectedDocs.add(
            new ExpectedDoc(
                "2",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":\"only\",\"required_kw\":\"present\"}",
                "only",
                "present",
                List.of(),
                "only",
                List.of()
            )
        );

        // doc 3: nullability violation — missing field accepted, marked in _ignored
        expectedDocs.add(
            new ExpectedDoc(
                "3",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":\"kept3\"}",
                "kept3",
                null,
                List.of("required_kw"),
                "kept3",
                List.of()
            )
        );

        // doc 4: nullability violation — explicit null accepted, marked in _ignored.
        // Synthetic source preserves the null slot written by the inline-null array-order binary doc-values column,
        // so _source.required_kw is [null] (an array), not absent. _ignored contains "required_kw".
        expectedDocs.add(
            new ExpectedDoc(
                "4",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":\"kept4\",\"required_kw\":null}",
                "kept4",
                Collections.singletonList(null),
                List.of("required_kw"),
                "kept4",
                List.of()
            )
        );

        // doc 5: nullability violation — empty array accepted, marked in _ignored
        expectedDocs.add(
            new ExpectedDoc(
                "5",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":\"kept5\",\"required_kw\":[]}",
                "kept5",
                null,
                List.of("required_kw"),
                "kept5",
                List.of()
            )
        );

        // doc 6: both violations in one document. For single_kw, the multi-value violation fires (multi_value:false)
        // and [a,b] is stored with "a" primary + "b" in the sidecar; single_kw appears in _ignored.
        // For required_kw, the [null,"real"] array contains "real", which satisfies nullability:false, so the field
        // is NOT marked in _ignored. Synthetic source preserves the null slot, so _source.required_kw is
        // [null,"real"] verbatim — the null is not discarded.
        expectedDocs.add(
            new ExpectedDoc(
                "6",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":[\"a\",\"b\"],\"required_kw\":[null,\"real\"]}",
                List.of("a", "b"),
                Arrays.asList(null, "real"),
                List.of("single_kw"),
                "a",
                List.of("b")
            )
        );

        for (ExpectedDoc doc : expectedDocs) {
            indexDoc(doc);
        }
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        flush(INDEX_NAME, true);

        verifyAll(expectedDocs);
        verifyMappingRoundTrip();

        clusterRollingUpgrade(nodeIndex -> {
            ensureGreen(INDEX_NAME);
            verifyAll(expectedDocs);
            verifyMappingRoundTrip();
        });

        // Post-upgrade: write new violating docs into the old-created index from a fully-upgraded cluster.
        // This exercises hazard 1: upgraded code must use the old index's created-version encoding.
        expectedDocs.add(
            new ExpectedDoc(
                "7",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":[\"new1\",\"new2\"],\"required_kw\":\"present\"}",
                List.of("new1", "new2"),
                "present",
                List.of("single_kw"),
                "new1",
                List.of("new2")
            )
        );
        expectedDocs.add(
            new ExpectedDoc(
                "8",
                "{\"@timestamp\":\"" + ts + "\",\"single_kw\":\"new-only\"}",
                "new-only",
                null,
                List.of("required_kw"),
                "new-only",
                List.of()
            )
        );
        for (ExpectedDoc doc : expectedDocs.subList(expectedDocs.size() - 2, expectedDocs.size())) {
            indexDoc(doc);
        }
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        verifyAll(expectedDocs);
        verifyMappingRoundTrip();
    }

    private void indexDoc(ExpectedDoc doc) throws IOException {
        var request = new Request("PUT", "/" + INDEX_NAME + "/_doc/" + doc.id());
        request.setJsonEntity(doc.body());
        assertOK(client().performRequest(request));
    }

    private void verifyAll(List<ExpectedDoc> docs) throws IOException {
        for (ExpectedDoc doc : docs) {
            verifySource(doc);
            verifyIgnored(doc);
            verifySidecarInvisibility(doc);
        }
    }

    @SuppressWarnings("unchecked")
    private void verifySource(ExpectedDoc doc) throws IOException {
        var request = new Request("GET", "/" + INDEX_NAME + "/_doc/" + doc.id());
        var response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> hit = entityAsMap(response);
        Map<String, Object> source = (Map<String, Object>) hit.get("_source");

        if (doc.expectedSingleKw() == null) {
            assertThat("doc " + doc.id() + " single_kw", source.get("single_kw"), nullValue());
        } else {
            assertThat("doc " + doc.id() + " single_kw", source.get("single_kw"), equalTo(doc.expectedSingleKw()));
        }

        if (doc.expectedRequiredKw() == null) {
            assertThat("doc " + doc.id() + " required_kw", source.get("required_kw"), nullValue());
        } else {
            assertThat("doc " + doc.id() + " required_kw", source.get("required_kw"), equalTo(doc.expectedRequiredKw()));
        }
    }

    @SuppressWarnings("unchecked")
    private void verifyIgnored(ExpectedDoc doc) throws IOException {
        var request = new Request("GET", "/" + INDEX_NAME + "/_doc/" + doc.id());
        request.addParameter("stored_fields", "_ignored");
        var response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> hit = entityAsMap(response);

        if (doc.expectedIgnored().isEmpty()) {
            assertThat("doc " + doc.id() + " _ignored should be absent", hit.get("_ignored"), nullValue());
        } else {
            List<String> ignored = (List<String>) hit.get("_ignored");
            assertThat("doc " + doc.id() + " _ignored", ignored, containsInAnyOrder(doc.expectedIgnored().toArray()));
        }
    }

    private void verifySidecarInvisibility(ExpectedDoc doc) throws IOException {
        // primary value must be findable
        assertTermQueryCount(doc.id() + " primary", "single_kw", doc.searchablePrimary(), 1);
        // sidecar values must NOT be findable via the primary column
        for (String sidecar : doc.sidecarValues()) {
            assertTermQueryCount(doc.id() + " sidecar=" + sidecar, "single_kw", sidecar, 0);
        }
    }

    private void assertTermQueryCount(String label, String field, String value, int expected) throws IOException {
        var request = new Request("GET", "/" + INDEX_NAME + "/_count");
        request.setJsonEntity("{\"query\":{\"term\":{\"" + field + "\":\"" + value + "\"}}}");
        var response = client().performRequest(request);
        assertOK(response);
        int count = ObjectPath.evaluate(entityAsMap(response), "count");
        assertThat("term query count for " + label, count, equalTo(expected));
    }

    private void verifyMappingRoundTrip() throws IOException {
        var request = new Request("GET", "/" + INDEX_NAME + "/_mapping");
        var response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> body = entityAsMap(response);
        assertThat(
            "single_kw.doc_values.multi_value",
            ObjectPath.evaluate(body, INDEX_NAME + ".mappings.properties.single_kw.doc_values.multi_value"),
            equalTo(false)
        );
        assertThat(
            "single_kw.doc_values.on_failure",
            ObjectPath.evaluate(body, INDEX_NAME + ".mappings.properties.single_kw.doc_values.on_failure"),
            equalTo("ignore")
        );
        assertThat(
            "required_kw.doc_values.nullability",
            ObjectPath.evaluate(body, INDEX_NAME + ".mappings.properties.required_kw.doc_values.nullability"),
            equalTo(false)
        );
        assertThat(
            "required_kw.doc_values.on_failure",
            ObjectPath.evaluate(body, INDEX_NAME + ".mappings.properties.required_kw.doc_values.on_failure"),
            equalTo("ignore")
        );
    }
}
