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

package org.elasticsearch.rest.action.admin.indices.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.bwcompat.StaticIndexBackwardCompatibilityTest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class UpgradeReallyOldIndexTest extends StaticIndexBackwardCompatibilityTest {

    public void testUpgrade_0_20() throws Exception {
        String indexName = "test";
        loadIndex("index-0.20.zip", indexName);
        assertMinVersion(indexName, org.apache.lucene.util.Version.parse("3.6.2"));
        assertTrue(UpgradeTest.hasAncientSegments(client(), indexName));
        UpgradeTest.assertNotUpgraded(client(), indexName);
        assertNoFailures(client().admin().indices().prepareUpgrade(indexName).setUpgradeOnlyAncientSegments(true).get());
        assertFalse(UpgradeTest.hasAncientSegments(client(), indexName));

        // This index has entirely ancient segments so the whole index should now be upgraded:
        UpgradeTest.assertUpgraded(client(), indexName);
        assertEquals(Version.CURRENT.luceneVersion.toString(), client().admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, IndexMetaData.SETTING_VERSION_MINIMUM_COMPATIBLE));
        assertMinVersion(indexName, Version.CURRENT.luceneVersion);

    }

    public void testUpgradeMixed_0_20_6_and_0_90_6() throws Exception {
        String indexName = "index-0.20.6-and-0.90.6";
        loadIndex(indexName + ".zip", indexName);
        assertMinVersion(indexName, org.apache.lucene.util.Version.parse("3.6.2"));
        // Has ancient segments?:
        assertTrue(UpgradeTest.hasAncientSegments(client(), indexName));

        // Also has "merely old" segments?:
        assertTrue(UpgradeTest.hasOldButNotAncientSegments(client(), indexName));

        // Needs upgrading?
        UpgradeTest.assertNotUpgraded(client(), indexName);
        
        // Now upgrade only the ancient ones:
        assertNoFailures(client().admin().indices().prepareUpgrade(indexName).setUpgradeOnlyAncientSegments(true).get());

        // No more ancient ones?
        assertFalse(UpgradeTest.hasAncientSegments(client(), indexName));

        // We succeeded in upgrading only the ancient segments but leaving the "merely old" ones untouched:
        assertTrue(UpgradeTest.hasOldButNotAncientSegments(client(), indexName));
        assertEquals(org.apache.lucene.util.Version.LUCENE_4_5_1.toString(), client().admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, IndexMetaData.SETTING_VERSION_MINIMUM_COMPATIBLE));
        assertMinVersion(indexName, org.apache.lucene.util.Version.LUCENE_4_5_1);

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
