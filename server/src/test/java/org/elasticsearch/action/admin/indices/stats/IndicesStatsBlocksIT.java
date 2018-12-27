/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndicesStatsBlocksIT extends ESIntegTestCase {

    public void testIndicesStatsWithBlocks() {
        createIndex("ro");
        ensureGreen("ro");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE)) {
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
            enableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
            client().admin().indices().prepareStats("ro").execute().actionGet();
            fail("Exists should fail when " + IndexMetaData.SETTING_BLOCKS_METADATA + " is true");
        } catch (ClusterBlockException e) {
            // Ok, a ClusterBlockException is expected
        } finally {
            disableIndexBlock("ro", IndexMetaData.SETTING_BLOCKS_METADATA);
        }
    }
}
