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

import org.elasticsearch.bwcompat.StaticIndexBackwardCompatibilityTest;
import org.elasticsearch.node.Node;

public class UpgradeReallyOldIndexTest extends StaticIndexBackwardCompatibilityTest {

    public void testUpgrade_0_90_6() throws Exception {
        String indexName = "index-0.90.6";
        loadIndex(indexName, Node.HTTP_ENABLED, true);
        
        UpgradeTest.assertNotUpgraded(httpClient(), indexName);
        assertTrue(UpgradeTest.hasAncientSegments(httpClient(), indexName));
        UpgradeTest.runUpgrade(httpClient(), indexName, "wait_for_completion", "true", "only_ancient_segments", "true");
        assertFalse(UpgradeTest.hasAncientSegments(httpClient(), "index-0.90.6"));
        // This index has only ancient segments, so it should now be fully upgraded:
        UpgradeTest.assertUpgraded(httpClient(), indexName);
    }

}
