/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aliases;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.rename.RenameIndexAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@TestLogging(value = "org.elasticsearch.indices.cluster.IndicesClusterStateService:DEBUG", reason = "log index renames")
public class IndexAliasRenameIT extends ESIntegTestCase {

    /**
     * Tests a simple index rename operation and verifies that documents are only accessible
     * in the new index after the rename.
     */
    public void testSimpleRename() {
        String original = "original";
        String newIndex = "new_index";

        // Index a document, creating the original index
        createIndex(original);
        client().prepareIndex(original).setSource("""
            {
              "field": "value"
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Ensure the document is searchable in the original index
        assertSearch(original, "value");
        assertNoSearchHits(newIndex);

        // Rename the index
        rename(original, newIndex);

        // Ensure the document is searchable in the new index
        assertNoSearchHits(original);
        assertSearch(newIndex, "value");
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
        assertSearch(index1, "value1");
        assertSearch(index2, "value2");

        // Rename both indices
        rename(index1, newIndex1);
        rename(index2, newIndex2);

        // Ensure documents are searchable in new indices
        assertSearch(newIndex1, "value1");
        assertSearch(newIndex2, "value2");

        // Ensure old indices are not searchable
        assertNoSearchHits(index1);
        assertNoSearchHits(index2);
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
        assertSearch(initialIndex, "test_value");

        // Rename alpha_index to beta_index
        rename(initialIndex, renamedOnceIndex);

        // Assert document is searchable in beta_index, not in alpha_index
        assertSearch(renamedOnceIndex, "test_value");
        assertNoSearchHits(initialIndex);

        // Rename beta_index to gamma_index
        rename(renamedOnceIndex, renamedTwiceIndex);

        // Assert document is searchable in gamma_index, not in previous indices
        assertSearch(renamedTwiceIndex, "test_value");
        assertNoSearchHits(renamedOnceIndex);
        assertNoSearchHits(initialIndex);
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
        assertSearch(indexA, "A");
        assertSearch(indexB, "B");

        // Rename indexA to tempIndex
        rename(indexA, tempIndex);

        // After first rename
        assertSearch(tempIndex, "A");
        assertSearch(indexB, "B");
        assertNoSearchHits(indexA);

        // Rename indexB to indexA
        rename(indexB, indexA);

        // After second rename
        assertSearch(tempIndex, "A");
        assertSearch(indexA, "B");
        assertNoSearchHits(indexB);

        // Rename tempIndex to indexB
        rename(tempIndex, indexB);

        // Final assertions
        assertSearch(indexB, "A");
        assertSearch(indexA, "B");
        assertNoSearchHits(tempIndex);
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
        assertSearch(original, "value");

        // Rename the original index
        rename(original, renamed);

        // Assert document is searchable in the renamed index, not in the original
        assertSearch(renamed, "value");
        assertNoSearchHits(original);

        // Create a new index with the original name and index a new document
        createIndex(original);
        client().prepareIndex(original)
            .setSource("{ \"field\": \"new_value\" }", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Assert the correct field values are returned for each index
        assertSearch(renamed, "value");
        assertSearch(original, "new_value");
    }

    /**
     * Tests changing the mapping of a renamed index and verifies that the new mapping
     * is applied correctly while still allowing access to existing documents.
     */
    public void testChangeMappingOfRenamedIndex() {
        String originalIndex = "original";
        String newIndex = "new_index";
        // Index a document, creating the original index
        createIndex(originalIndex);
        client().prepareIndex(originalIndex).setSource("""
            {
              "foo": "bar", "baz": 123
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Ensure the document is searchable in the original index
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(originalIndex), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(newIndex).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);

        // Rename the index
        rename(originalIndex, newIndex);

        // Ensure the document is searchable in the new index
        ElasticsearchAssertions.assertHitCount(
            client().prepareSearch(originalIndex).setIndicesOptions(IndicesOptions.lenientExpandOpen()),
            0L
        );
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(newIndex), 1L);

        PutMappingRequest putMappingRequest = new PutMappingRequest(newIndex);
        putMappingRequest.source("field", "type=keyword");
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(putMappingRequest));
    }

    /**
    * Tests that a random block added to an index persists after renaming,
    * can be removed from the renamed index, and is no longer present.
    */
    public void testIndexBlockPersistsThroughRenameAndCanBeRemoved() {
        String original = "block_test_index";
        String renamed = "block_test_renamed";
        createIndex(original);

        // The read-only-allow-delete block cannot be added via the add index block API.
        var randomBlock = randomValueOtherThan(
            IndexMetadata.APIBlock.READ_ONLY_ALLOW_DELETE,
            () -> randomFrom(IndexMetadata.APIBlock.values())
        );

        // Add the block to the original index
        ElasticsearchAssertions.assertAcked(client().admin().indices().prepareAddBlock(randomBlock, original));

        // Ensure the block exists on the original index
        assertIndexBlock(original, randomBlock, true);

        // Rename the index
        rename(original, renamed);

        // Ensure the block exists on the renamed index
        assertIndexBlock(renamed, randomBlock, true);

        // Remove the block from the renamed index
        ElasticsearchAssertions.assertAcked(
            client().admin().indices().prepareRemoveBlock(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomBlock, renamed)
        );

        // Ensure no blocks exist on the renamed index
        for (var block : IndexMetadata.APIBlock.values()) {
            assertIndexBlock(renamed, block, false);
        }
    }

    /**
    * Tests cyclic renaming of a single index: foo -> bar -> foo.
    * Verifies that the document is accessible under the new name after each rename.
    */
    public void testCyclicRenameSingleIndexTwoSteps() {
        String indexFoo = "cyclic_foo";
        String indexBar = "cyclic_bar";

        // Create the initial index and index a document
        createIndex(indexFoo);
        client().prepareIndex(indexFoo)
            .setSource("{ \"field\": \"foo\" }", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Initial assertion
        assertSearch(indexFoo, "foo");

        // Step 1: foo -> bar
        rename(indexFoo, indexBar);
        assertNoSearchHits(indexFoo);
        assertSearch(indexBar, "foo");

        // Step 2: bar -> foo
        rename(indexBar, indexFoo);
        assertNoSearchHits(indexBar);
        assertSearch(indexFoo, "foo");
    }

    /**
    * Tests that renaming an index to the same name fails with an error.
    */
    public void testRenameIndexToSameNameFails() {
        String index = "same_name_index";
        createIndex(index);
        client().prepareIndex(index)
            .setSource("{ \"field\": \"value\" }", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Exception exception = expectThrows(ActionRequestValidationException.class, () -> rename(index, index));
        assertThat(
            "Exception message should indicate renaming to the same name is not allowed",
            exception.getMessage(),
            Matchers.containsString("source and destination indices must be different")
        );
    }

    /**
     * Tests that renaming an index to an empty string or null fails with a validation error.
     */
    public void testRenameIndexToEmptyFails() {
        String index = "invalid_rename_index";
        createIndex(index);

        Exception emptyException = expectThrows(ActionRequestValidationException.class, () -> rename(index, ""));
        assertThat(
            "Exception message should indicate destination index name cannot be empty",
            emptyException.getMessage(),
            Matchers.containsString("destination index is missing")
        );

        Exception nullException = expectThrows(ActionRequestValidationException.class, () -> rename(index, null));
        assertThat(
            "Exception message should indicate destination index name cannot be null",
            nullException.getMessage(),
            Matchers.containsString("destination index is missing")
        );
    }

    private void rename(String source, String target) {
        var response = client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, source, target)
        ).actionGet();
        assertTrue("Rename response should be acknowledged", response.isAcknowledged());
    }

    private void assertSearch(String index, String expectedFieldValue) {
        ElasticsearchAssertions.assertResponse(client().prepareSearch(index), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertEquals(index, response.getHits().getAt(0).getIndex());
            assertEquals(expectedFieldValue, response.getHits().getAt(0).getSourceAsMap().get("field"));
        });
    }

    private void assertNoSearchHits(String index) {
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(index).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
    }

    private void assertIndexBlock(String index, IndexMetadata.APIBlock block, boolean shouldHaveBlock) {
        final ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final ProjectId projectId = ProjectId.DEFAULT;
        final IndexMetadata indexMetadata = clusterState.metadata().getProject(projectId).indices().get(index);
        final Settings indexSettings = indexMetadata.getSettings();
        assertThat(indexSettings.hasValue(block.settingName()), is(shouldHaveBlock));
        assertThat(indexSettings.getAsBoolean(block.settingName(), false), is(shouldHaveBlock));
        assertThat(clusterState.blocks().hasIndexBlock(projectId, index, block.getBlock()), is(shouldHaveBlock));
        assertThat(
            "Index " + index + " must have only 1 block with [id=" + block.getBlock().id() + "]",
            clusterState.blocks()
                .indices(projectId)
                .getOrDefault(index, emptySet())
                .stream()
                .filter(clusterBlock -> clusterBlock.id() == block.getBlock().id())
                .count(),
            equalTo(shouldHaveBlock ? 1L : 0L)
        );
        if (block.getBlock().contains(ClusterBlockLevel.WRITE)) {
            assertThat(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.get(indexSettings), is(shouldHaveBlock));
        }
    }
}
