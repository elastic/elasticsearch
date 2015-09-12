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

package org.elasticsearch.action.admin.indices.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.bwcompat.StaticIndexBackwardCompatibilityIT;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;

public class UpgradeReallyOldIndexIT extends StaticIndexBackwardCompatibilityIT {

    public void testUpgrade_0_90_6() throws Exception {
        String indexName = "index-0.90.6";

        loadIndex(indexName);
        assertMinVersion(indexName, org.apache.lucene.util.Version.parse("4.5.1"));
        UpgradeIT.assertNotUpgraded(client(), indexName);
        assertTrue(UpgradeIT.hasAncientSegments(client(), indexName));
        assertNoFailures(client().admin().indices().prepareUpgrade(indexName).setUpgradeOnlyAncientSegments(true).get());

        assertFalse(UpgradeIT.hasAncientSegments(client(), indexName));
        // This index has only ancient segments, so it should now be fully upgraded:
        UpgradeIT.assertUpgraded(client(), indexName);
        assertEquals(Version.CURRENT.luceneVersion.toString(), client().admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, IndexMetaData.SETTING_VERSION_MINIMUM_COMPATIBLE));
        assertMinVersion(indexName, Version.CURRENT.luceneVersion);

        assertEquals(client().admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, IndexMetaData.SETTING_VERSION_UPGRADED), Integer.toString(Version.CURRENT.id));
    }

    public void testUpgradeConflictingMapping() throws Exception {
        String indexName = "index-conflicting-mappings-1.7.0";
        logger.info("Checking static index " + indexName);
        Settings nodeSettings = prepareBackwardsDataDir(getDataPath(indexName + ".zip"));
        try {
            internalCluster().startNode(nodeSettings);
            fail("Should have failed to start the node");
        } catch (Exception ex) {
            assertThat(ex.getMessage(), containsString("conflicts with existing mapping in other types"));
        }
    }

    private void assertMinVersion(String index, org.apache.lucene.util.Version version) {
        for (IndicesService services : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = services.indexService(index);
            if (indexService != null) {
                assertEquals(version, indexService.shard(0).minimumCompatibleVersion());
            }
        }

    }

}
