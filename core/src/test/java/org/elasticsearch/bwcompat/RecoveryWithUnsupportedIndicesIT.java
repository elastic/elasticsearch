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
package org.elasticsearch.bwcompat;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import static org.hamcrest.Matchers.containsString;

public class RecoveryWithUnsupportedIndicesIT extends StaticIndexBackwardCompatibilityIT {
    public void testUpgradeStartClusterOn_0_20_6() throws Exception {
        String indexName = "unsupported-0.20.6";

        logger.info("Checking static index {}", indexName);
        Settings nodeSettings = prepareBackwardsDataDir(getBwcIndicesPath().resolve(indexName + ".zip"), NetworkModule.HTTP_ENABLED.getKey(), true);
        try {
            internalCluster().startNode(nodeSettings);
            fail();
        } catch (Exception ex) {
            assertThat(ex.getMessage(), containsString(" was created before v2.0.0.beta1 and wasn't upgraded"));
        }
    }
}
