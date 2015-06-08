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

    public void testUpgrade_0_90_6() throws Exception {
        String indexName = "index-0.90.6";

        loadIndex(indexName);
        assertMinVersion(indexName, org.apache.lucene.util.Version.parse("4.5.1"));
        UpgradeTest.assertNotUpgraded(client(), indexName);
        assertTrue(UpgradeTest.hasAncientSegments(client(), indexName));
        assertNoFailures(client().admin().indices().prepareUpgrade(indexName).setUpgradeOnlyAncientSegments(true).get());

        assertFalse(UpgradeTest.hasAncientSegments(client(), "index-0.90.6"));
        // This index has only ancient segments, so it should now be fully upgraded:
        UpgradeTest.assertUpgraded(client(), indexName);
        assertEquals(Version.CURRENT.luceneVersion.toString(), client().admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, IndexMetaData.SETTING_VERSION_MINIMUM_COMPATIBLE));
        assertMinVersion(indexName, Version.CURRENT.luceneVersion);
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
