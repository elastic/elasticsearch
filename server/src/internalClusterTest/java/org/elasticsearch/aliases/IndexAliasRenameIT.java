/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aliases;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;

public class IndexAliasRenameIT extends ESSingleNodeTestCase {

    public void testSimpleRename() {
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
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.renameIndex().index(originalIndex).destination(newIndex));
        IndicesAliasesResponse response = client().execute(TransportIndicesAliasesAction.TYPE, request).actionGet();
        assertFalse(response.hasErrors());

        // Ensure the new index exists and is healthy
        ensureGreen(newIndex);

        // Ensure the document is searchable in the new index
        ElasticsearchAssertions.assertHitCount(
            client().prepareSearch(originalIndex).setIndicesOptions(IndicesOptions.lenientExpandOpen()),
            0L
        );
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(newIndex), 1L);
    }

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
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.renameIndex().index(index1).destination(newIndex1));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.renameIndex().index(index2).destination(newIndex2));
        IndicesAliasesResponse response = client().execute(TransportIndicesAliasesAction.TYPE, request).actionGet();
        assertFalse(response.hasErrors());

        // Ensure new indices exist and are healthy
        ensureGreen(newIndex1, newIndex2);

        // Ensure documents are searchable in new indices
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(newIndex1), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(newIndex2), 1L);

        // Ensure old indices are not searchable
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(index1).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(index2).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
    }

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
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.renameIndex().index(originalIndex).destination(newIndex));
        IndicesAliasesResponse response = client().execute(TransportIndicesAliasesAction.TYPE, request).actionGet();
        assertFalse(response.hasErrors());

        // Ensure the new index exists and is healthy
        ensureGreen(newIndex);

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
}
