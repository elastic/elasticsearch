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

package org.elasticsearch.action.admin.indices.mapping.delete;

import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class DeleteMappingBlocksTests extends ElasticsearchIntegrationTest {

    public void testDeleteMappingWithBlocks() {
        assertAcked(client().admin().indices().prepareCreate("test-blocks").addMapping("type-1", "field1", "type=string").get());

        // Request is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test-blocks", blockSetting);
                assertBlocked(client().admin().indices().prepareDeleteMapping("test-blocks").setType("type-1"));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().indices().prepareDeleteMapping("test-blocks").setType("type-1"));
        } finally {
            setClusterReadOnly(false);
        }

        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_READ);
            assertAcked(client().admin().indices().prepareDeleteMapping("test-blocks").setType("type-1"));
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_READ);
        }
    }
}
