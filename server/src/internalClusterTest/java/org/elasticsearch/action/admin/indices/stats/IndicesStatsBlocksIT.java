/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndicesStatsBlocksIT extends ESIntegTestCase {

    public void testIndicesStatsWithBlocks() {
        createIndex("ro");
        ensureGreen("ro");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("ro", blockSetting);
                IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("ro").execute().actionGet();
                assertNotNull(indicesStatsResponse.getIndex("ro"));
            } finally {
                disableIndexBlock("ro", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
            client().admin().indices().prepareStats("ro").execute().actionGet();
            fail("Exists should fail when " + IndexMetadata.SETTING_BLOCKS_METADATA + " is true");
        } catch (ClusterBlockException e) {
            // Ok, a ClusterBlockException is expected
        } finally {
            disableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
        }
    }
}
