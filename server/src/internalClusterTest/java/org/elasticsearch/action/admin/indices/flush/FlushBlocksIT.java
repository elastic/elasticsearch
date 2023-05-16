/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class FlushBlocksIT extends ESIntegTestCase {
    public void testFlushWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        NumShards numShards = getNumShards("test");

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test").setId("" + i).setSource("test", "init").execute().actionGet();
        }

        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_BLOCKS_METADATA,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("test", blockSetting);
                FlushResponse response = indicesAdmin().prepareFlush("test").execute().actionGet();
                assertNoFailures(response);
                assertThat(response.getSuccessfulShards(), equalTo(numShards.totalNumShards));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }
}
