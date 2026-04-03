/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for <em>manual</em> reindex slicing.
 * <p>
 * During manual reindexing, the client issues separate {@link ReindexRequest}s, each specifying the
 * {@link SearchSourceBuilder#slice(SliceBuilder) slice} on the search body rather than
 * relying on the {@code slices} URL parameter to fan out parallel workers in one request.
 * Each slice restricts the search to a partition of matching documents. Running slice
 * {@code 0..max-1} in separate reindex calls eventually covers the same document set as a
 * single unsliced reindex, as long as the source query and slice parameters are consistent
 * across calls.es.
 */
public class ManualSlicingReindexIT extends ESRestTestCase {

    /**
     * Tests that reindexing can handle bulk updates to the source index during reindexing.
     * The test uses a single source index and two destination indices. The test uses two slices.
     * By reindexing each slice both pre/post bulk update, we assert that updating the source during
     * reindexing does not lead to any documents in the source being reindexed more than once.
     * The test works as follows:
     * 1. Reindex slice 0 to dest1
     * 2. Reindex slice 1 to dest2
     * 3. The source is then bulk-updated with a new field
     * 4. Reindex slice 1 to dest1 (documents in this slice will have been updated)
     * 5. Reindex slice 0 to dest2 (documents in this slice will have been updated)
     * 6. Assert the two destination indices are identical. Since we're setting the op_type to
     * 'create', Each reindex response must report no version conflicts or failures, and each
     * destination must contain exactly {@code docCount} documents with ids {@code 0}..{@code docCount - 1}.
     *
     * @throws IOException if a REST request fails
     */
    public void testReindexWithSourceUpdatesBetweenSlices() throws IOException {
        int docCount = randomIntBetween(500, 1000);
        String sourceIndex = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);
        String dest1 = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);
        String dest2 = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);

        createIndex(sourceIndex, Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        bulkIndexSource(sourceIndex, docCount, 0);
        assertDocCount(client(), sourceIndex, docCount);

        Map<String, Object> r0 = performReindex(sourceIndex, dest1, 0, 2);
        assertNoReindexConflicts("slice 0 to dest1", r0);
        assertTrue("slice 0 to dest1 should index some docs", reindexCreated(r0) > 0);

        Map<String, Object> r1 = performReindex(sourceIndex, dest2, 1, 2);
        assertNoReindexConflicts("slice 1 to dest2", r1);
        assertTrue("slice 1 to dest2 should index some docs", reindexCreated(r1) > 0);

        bulkIndexSource(sourceIndex, docCount, 1);

        Map<String, Object> r2 = performReindex(sourceIndex, dest1, 1, 2);
        assertNoReindexConflicts("slice 1 to dest1 after bulk", r2);
        assertTrue("slice 1 to dest1 should index some docs", reindexCreated(r2) > 0);

        Map<String, Object> r3 = performReindex(sourceIndex, dest2, 0, 2);
        assertNoReindexConflicts("slice 0 to dest2 after bulk", r3);
        assertTrue("slice 0 to dest2 should index some docs", reindexCreated(r3) > 0);

        assertDestinationHasAllDocIds("dest1", dest1, docCount);
        assertDestinationHasAllDocIds("dest2", dest2, docCount);
    }

    /**
     * Executes {@code POST /_reindex} with {@code refresh=true}, manual slicing and {@code dest.op_type = create}.
     *
     * @param sourceIndex index to read from
     * @param destIndex   index to write to
     * @param sliceId     slice id ({@code 0}..{@code sliceMax - 1})
     * @param sliceMax    slice count
     * @return parsed JSON body (e.g. {@code created}, {@code version_conflicts}, {@code failures})
     * @throws IOException if the request fails
     */
    private static Map<String, Object> performReindex(String sourceIndex, String destIndex, int sliceId, int sliceMax) throws IOException {
        Request request = new Request("POST", "/_reindex");
        request.addParameter("refresh", "true");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "source": {
                "index": "%s",
                "slice": {
                  "id": %d,
                  "max": %d
                }
              },
              "dest": {
                "index": "%s",
                "op_type": "create"
              }
            }
            """, sourceIndex, sliceId, sliceMax, destIndex));
        Response response = assertOK(client().performRequest(request));
        return entityAsMap(response);
    }

    /**
     * Returns the {@code created} count from a reindex JSON response.
     *
     * @param response body map from {@link #performReindex}
     * @return number of documents created in that call
     * @throws AssertionError if {@code created} is missing or not numeric
     */
    private static long reindexCreated(Map<String, Object> response) {
        Object c = response.get("created");
        if (c instanceof Number n) {
            return n.longValue();
        }
        throw new AssertionError("reindex response missing numeric [created]: " + response);
    }

    /**
     * Asserts that a reindex response has no version conflicts and no top-level
     * {@code failures} entries.
     *
     * @param sliceLabel context for assertion messages (e.g. which slice and destination)
     * @param response   body map from {@code POST /_reindex}
     */
    private static void assertNoReindexConflicts(String sliceLabel, Map<String, Object> response) {
        Object vc = response.getOrDefault("version_conflicts", 0);
        long versionConflicts = vc instanceof Number n ? n.longValue() : Long.parseLong(vc.toString());
        assertEquals(sliceLabel + " should have no version conflicts", 0L, versionConflicts);
        @SuppressWarnings("unchecked")
        List<Object> failures = (List<Object>) response.getOrDefault("failures", List.of());
        assertThat(sliceLabel + " failures", failures, empty());
    }

    /**
     * Asserts the destination index has exactly {@code docCount} documents, that all hit
     * {@code _id} values are distinct, and that every id from {@code "0"} to
     * {@code String.valueOf(docCount - 1)} is present (matching {@link #bulkIndexSource}).
     *
     * @param label     context for assertion messages
     * @param destIndex destination index name
     * @param docCount  expected document count and id range
     * @throws IOException if search requests fail
     */
    private void assertDestinationHasAllDocIds(String label, String destIndex, int docCount) throws IOException {
        assertDocCount(client(), destIndex, docCount);

        Request search = new Request("POST", "/" + destIndex + "/_search");
        search.addParameter("rest_total_hits_as_int", "true");
        search.setJsonEntity(String.format(Locale.ROOT, """
            {
              "size": %d,
              "query": { "match_all": {} }
            }
            """, docCount));
        Response searchResponse = assertOK(client().performRequest(search));
        ObjectPath path = ObjectPath.createFromResponse(searchResponse);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) path.evaluate("hits.hits");
        assertEquals(label + " should have exactly " + docCount + " unique doc IDs", docCount, hits.size());
        Set<String> destIds = new HashSet<>();
        for (Map<String, Object> hit : hits) {
            destIds.add(hit.get("_id").toString());
        }
        assertEquals(label + " unique ids", docCount, destIds.size());
        for (int i = 0; i < docCount; i++) {
            assertTrue(label + " should contain doc " + i, destIds.contains(String.valueOf(i)));
        }
    }

    /**
     * Indexes {@code docCount} documents with ids {@code "0"}..{@code String.valueOf(docCount - 1)} via
     * bulk index actions. The {@code pass} selects the document shape: initial load,
     * update with {@code updated}, or update with {@code updated again}
     *
     * @param sourceIndex index to bulk into
     * @param docCount    number of documents (ids {@code 0}..{@code docCount - 1})
     * @param pass        {@code 0} initial, {@code 1} first update, {@code 2} second update
     * @throws IOException if the bulk request fails
     */
    private void bulkIndexSource(String sourceIndex, int docCount, int pass) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < docCount; i++) {
            bulk.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            switch (pass) {
                case 0 -> bulk.append("{\"foo\":\"v").append(i).append("\"}\n");
                case 1 -> bulk.append("{\"foo\":\"v").append(i).append("\",\"updated\":true}\n");
                case 2 -> bulk.append("{\"foo\":\"v").append(i).append("\",\"updated again\":true}\n");
                default -> throw new IllegalArgumentException("pass");
            }
        }
        Request bulkRequest = new Request("POST", "/" + sourceIndex + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        assertOK(client().performRequest(bulkRequest));
    }
}
