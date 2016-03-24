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

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.snapshots.SnapshotSharedTest;
import org.elasticsearch.snapshots.SnapshotSharedTest.AfterSnapshotAction;
import org.elasticsearch.test.ESBackcompatTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

@TestLogging("level:DEBUG") // hack to enable root level debug logic. TODO fix to be "_root:DEBUG"
public class SnapshotBackwardsCompatibilityIT extends ESBackcompatTestCase {
    /**
     * Tests that we can restore a snapshot made by a mixed version cluster when
     * we're on a modern version cluster.
     */
    public void testBasicWorkflow() {
        SnapshotSharedTest.testBasicWorkflow(logger, this, frequentlyUpgradeCluster,
                either(equalTo(backwardsCompatibilityVersion())).or(equalTo(Version.CURRENT)));
    }

    public void testSnapshotMoreThanOnce() throws Exception {
        SnapshotSharedTest.testSnapshotMoreThanOnce(logger, this, frequentlyUpgradeCluster, 0);
    }

    AfterSnapshotAction frequentlyUpgradeCluster = new AfterSnapshotAction() {
        @Override
        public void run(String[] indices, int numDocs) {
            if (frequently()) {
                upgradeCluster(indices, numDocs);
            }
        }
    };

    private void upgradeCluster(String[] indices, int numDocs) {
        logger.info("-->  upgrade");
        disableAllocation(indices);
        backwardsCluster().allowOnAllNodes(indices);
        logClusterState();
        boolean upgraded;
        do {
            logClusterState();
            SearchResponse countResponse = client().prepareSearch().setSize(0).get();
            assertHitCount(countResponse, numDocs);
            try {
                upgraded = backwardsCluster().upgradeOneNode();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
            ensureYellow();
            countResponse = client().prepareSearch().setSize(0).get();
            assertHitCount(countResponse, numDocs);
        } while (upgraded);
        enableAllocation(indices);
    }
}
