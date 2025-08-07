/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aliases;

import org.elasticsearch.action.admin.indices.rename.RenameIndexAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentType;

@TestLogging(value = "org.elasticsearch.indices.cluster.IndicesClusterStateService:DEBUG", reason = "log index renames")
public class IndexAliasRenameIT extends ESIntegTestCase {
    private static final String ORIGINAL = "original";
    private static final String NEW_INDEX = "new_index";

    /**
     * Tests a simple index rename operation and verifies that documents are only accessible
     * in the new index after the rename.
     */
    public void testSimpleRename() {
        // Index a document, creating the original index
        createIndex(ORIGINAL);
        client().prepareIndex(ORIGINAL).setSource("""
            {
              "foo": "bar", "baz": 123
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Ensure the document is searchable in the original index
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(ORIGINAL), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(NEW_INDEX).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);

        // Rename the index
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, ORIGINAL, NEW_INDEX)
        ).actionGet();

        // Ensure the new index exists and is healthy
        ensureGreen(NEW_INDEX);

        // Ensure the document is searchable in the new index
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(ORIGINAL).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(NEW_INDEX), 1L);
    }

    /**
     * Tests renaming multiple indices and verifies that documents are only accessible
     * in the new indices after the renames.
     */
    public void testRenameMultipleIndices() {
        String index1 = "index1";
        String index2 = "index2";
        String newIndex1 = "new_index1";
        String newIndex2 = "new_index2";

        // Create two indices and index a document in each
        createIndex(index1);
        createIndex(index2);

        client().prepareIndex(index1).setSource("""
                { "field": "value1" }
            """, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        client().prepareIndex(index2).setSource("""
                { "field": "value2" }
            """, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Ensure documents are searchable in original indices
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(index1), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(index2), 1L);

        // Rename both indices
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, index1, newIndex1)
        ).actionGet();

        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, index2, newIndex2)
        ).actionGet();

        // Ensure new indices exist and are healthy
        ensureGreen(newIndex1, newIndex2);

        // Ensure documents are searchable in new indices
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(newIndex1), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(newIndex2), 1L);

        // Ensure old indices are not searchable
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(index1).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(index2).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
    }

    /**
     * Tests renaming an index twice with distinct names and verifies that documents
     * are only accessible in the latest renamed index.
     */
    public void testRenameIndexTwiceWithDistinctNames() {
        String initialIndex = "index_1";
        String renamedOnceIndex = "index_2";
        String renamedTwiceIndex = "index_3";

        // Create the initial index and index a document
        createIndex(initialIndex);
        client().prepareIndex(initialIndex).setSource("""
                { "field": "test_value" }
            """, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Assert document is searchable in the initial index
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(initialIndex), 1L);

        // Rename alpha_index to beta_index
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, initialIndex, renamedOnceIndex)
        ).actionGet();

        // Ensure beta_index exists and is healthy
        ensureGreen(renamedOnceIndex);

        // Assert document is searchable in beta_index, not in alpha_index
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamedOnceIndex), 1L);
        ElasticsearchAssertions.assertHitCount(
            client().prepareSearch(initialIndex).setIndicesOptions(IndicesOptions.lenientExpandOpen()),
            0L
        );

        // Rename beta_index to gamma_index
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, renamedOnceIndex, renamedTwiceIndex)
        ).actionGet();

        // Ensure gamma_index exists and is healthy
        ensureGreen(renamedTwiceIndex);

        // Assert document is searchable in gamma_index, not in previous indices
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamedTwiceIndex), 1L);
        ElasticsearchAssertions.assertHitCount(
            client().prepareSearch(renamedOnceIndex).setIndicesOptions(IndicesOptions.lenientExpandOpen()),
            0L
        );
        ElasticsearchAssertions.assertHitCount(
            client().prepareSearch(initialIndex).setIndicesOptions(IndicesOptions.lenientExpandOpen()),
            0L
        );
    }

    /**
     * Tests swapping the names of two indices via a series of renames using a temporary index name,
     * and verifies that documents are accessible under the swapped names.
     */
    public void testSwapIndexNamesViaRenames() {
        String indexA = "index_a";
        String indexB = "index_b";
        String tempIndex = "temp_index";

        // Create two indices and index a document in each
        createIndex(indexA);
        createIndex(indexB);

        client().prepareIndex(indexA)
            .setSource("{ \"field\": \"A\" }", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex(indexB)
            .setSource("{ \"field\": \"B\" }", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Initial assertions
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexA), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexB), 1L);

        // Rename indexA to tempIndex
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, indexA, tempIndex)
        ).actionGet();

        // After first rename
        ensureGreen(tempIndex, indexB);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(tempIndex), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexB), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexA).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);

        // Rename indexB to indexA
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, indexB, indexA)
        ).actionGet();

        // After second rename
        ensureGreen(tempIndex, indexA);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(tempIndex), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexA), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexB).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);

        // Rename tempIndex to indexB
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, tempIndex, indexB)
        ).actionGet();

        // Ensure indices exist and are healthy
        ensureGreen(indexA, indexB);

        // Final assertions
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexA), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(indexB), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(tempIndex).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
    }

    /**
     * Tests renaming an index and then recreating a new index with the original name,
     * ensuring that documents are correctly separated between the renamed and newly created indices.
     */
    public void testRenameAndRecreateOriginalIndex() {
        String original = "original_index";
        String renamed = "renamed_index";

        // Create the original index and index a document
        createIndex(original);
        client().prepareIndex(original)
            .setSource("{ \"field\": \"value\" }", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Assert document is searchable in the original index
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(original), 1L);

        // Rename the original index
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, original, renamed)
        ).actionGet();

        // Assert document is searchable in the renamed index, not in the original
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(original).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);

        // Create a new index with the original name and index a new document
        createIndex(original);
        client().prepareIndex(original)
            .setSource("{ \"field\": \"new_value\" }", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Assert the correct field values are returned for each index
        ElasticsearchAssertions.assertResponse(client().prepareSearch(renamed), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertEquals(renamed, response.getHits().getAt(0).getIndex());
            assertEquals("value", response.getHits().getAt(0).getSourceAsMap().get("field"));
        });
        ElasticsearchAssertions.assertResponse(client().prepareSearch(original), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertEquals(original, response.getHits().getAt(0).getIndex());
            assertEquals("new_value", response.getHits().getAt(0).getSourceAsMap().get("field"));
        });
    }
}
