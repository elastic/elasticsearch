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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;

public class IndexAliasRenameIT extends ESSingleNodeTestCase {
    private static final String ORIGINAL = "original";
    private static final String NEW_INDEX = "new_index";

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
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.renameIndex().index(ORIGINAL).destination(NEW_INDEX));
        IndicesAliasesResponse response = client().execute(TransportIndicesAliasesAction.TYPE, request).actionGet();
        assertFalse(response.hasErrors());

        // Ensure the new index exists and is healthy
        ensureGreen(NEW_INDEX);

        // Ensure the document is searchable in the new index
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(ORIGINAL).setIndicesOptions(IndicesOptions.lenientExpandOpen()), 0L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(NEW_INDEX), 1L);
    }
}
