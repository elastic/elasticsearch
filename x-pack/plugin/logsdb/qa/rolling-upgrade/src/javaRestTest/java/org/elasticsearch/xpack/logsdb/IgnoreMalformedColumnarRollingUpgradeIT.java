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
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * BWC test that verifies malformed values in strict-columnar (logsdb_columnar / columnar) indices
 * survive a rolling upgrade with their synthetic source intact.
 */
public class IgnoreMalformedColumnarRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    private static final String INDEX_NAME = "bwc-ignore-malformed-columnar";

    /**
     * The {@code columnar} index mode (and its logsdb-flavoured sibling {@code logsdb_columnar})
     * were introduced as tech preview in 9.5.0.  Strict-columnar behaviour – and therefore this
     * BWC scenario – does not apply to older clusters.
     */
    private static final String COLUMNAR_MODES_MIN_VERSION = "9.5.0";

    public void testIgnoreMalformedSurvivesUpgrade() throws Exception {
        var version = System.getProperty("tests.old_cluster_version") != null
            ? org.elasticsearch.test.cluster.util.Version.fromString(System.getProperty("tests.old_cluster_version"))
            : org.elasticsearch.test.cluster.util.Version.CURRENT;

        LuceneTestCase.assumeTrue(
            "columnar index modes require old cluster >= " + COLUMNAR_MODES_MIN_VERSION,
            version.onOrAfter(org.elasticsearch.test.cluster.util.Version.fromString(COLUMNAR_MODES_MIN_VERSION))
        );

        // Create a columnar index with a long field that allows ignore_malformed.
        // replica count of 1 ensures the shard remains accessible during rolling upgrade while
        // one node at a time is restarted (0 replicas would make the shard unavailable on restart).
        createIndex(INDEX_NAME, Settings.builder().put("index.mode", "columnar").put("index.number_of_replicas", 1).build(), """
            {
              "properties": {
                "value": {
                  "type": "long",
                  "ignore_malformed": true
                }
              }
            }
            """);

        // Doc 1: a well-formed long value.
        indexDocument("1", """
            {"value": 42}
            """);

        // Doc 2: a malformed value that cannot be parsed as a long. Old code stores this in the
        // per-field ._ignore_malformed binary doc-values column. New code must still read that
        // column for indices whose created-version pre-dates the merge into ._on_failure.
        indexDocument("2", """
            {"value": "not-a-number"}
            """);

        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Verify source before the upgrade: both values must be present.
        verifyDocSource("1", 42);
        verifyDocSource("2", "not-a-number");

        clusterRollingUpgrade(nodeIndex -> {
            ensureGreen(INDEX_NAME);
            // After each node is upgraded, re-verify that the sources are intact.
            // Without the index-version guard in addFallbackLayers(), doc 2's _source will be
            // missing the "value" field on the upgraded node because the new read path looks in
            // ._on_failure but the value was written to ._ignore_malformed by the old code.
            verifyDocSource("1", 42);
            verifyDocSource("2", "not-a-number");
        });

        // After the full upgrade, also verify a newly-written malformed doc round-trips correctly.
        // The write path (FallbackPostMapper) also has no index-version guard and would write to
        // ._on_failure; with the fix, the old index's created-version must gate the write path too.
        indexDocument("3", """
            {"value": "also-not-a-number"}
            """);
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        verifyDocSource("3", "also-not-a-number");
    }

    private void indexDocument(String id, String body) throws IOException {
        var request = new Request("PUT", "/" + INDEX_NAME + "/_doc/" + id);
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    /**
     * Fetches a single document by id using an {@code ids} query (no fielddata required) and
     * asserts that its synthetic {@code _source} contains {@code "value": expectedValue}.
     */
    private void verifyDocSource(String id, Object expectedValue) throws IOException {
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
        assertThat(
            "doc [" + id + "] value must be present in synthetic source (expected " + expectedValue + ")",
            source.get("value"),
            equalTo(expectedValue)
        );
    }
}
