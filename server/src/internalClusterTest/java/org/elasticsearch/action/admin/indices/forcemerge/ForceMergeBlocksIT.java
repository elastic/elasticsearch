/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ForceMergeBlocksIT extends ESIntegTestCase {

    public void testForceMergeWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        NumShards numShards = getNumShards("test");

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test").setId("" + i).setSource("test", "init").execute().actionGet();
        }

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);
                ForceMergeResponse response = client().admin().indices().prepareForceMerge("test").execute().actionGet();
                assertNoFailures(response);
                assertThat(response.getSuccessfulShards(), equalTo(numShards.totalNumShards));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Request is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA, SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareForceMerge("test"));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Merging all indices is blocked when the cluster is read-only
        try {
            ForceMergeResponse response = client().admin().indices().prepareForceMerge().execute().actionGet();
            assertNoFailures(response);
            assertThat(response.getSuccessfulShards(), equalTo(numShards.totalNumShards));

            setClusterReadOnly(true);
            assertBlocked(client().admin().indices().prepareForceMerge());
        } finally {
            setClusterReadOnly(false);
        }
    }
}
