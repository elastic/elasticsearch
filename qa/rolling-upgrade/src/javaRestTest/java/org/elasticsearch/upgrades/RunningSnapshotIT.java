/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.upgrades.SnapshotBasedRecoveryIT.indexDocs;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RunningSnapshotIT extends AbstractRollingUpgradeTestCase {

    public RunningSnapshotIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testRunningSnapshotCompleteAfterUpgrade() throws Exception {
        final String indexName = "index";
        final String repositoryName = "repo";
        final String snapshotName = "snapshot";
        final var nodeIds = getNodesInfo(client()).keySet();

        if (isOldCluster()) {
            registerRepository(repositoryName, "fs", randomBoolean(), Settings.builder().put("location", "backup").build());
            // create an index to have one shard per node
            createIndex(indexName, indexSettings(3, 0).put("index.routing.allocation.total_shards_per_node", 1).build());
            ensureGreen(indexName);
            if (randomBoolean()) {
                indexDocs(indexName, between(10, 50));
            }
            flush(indexName, true);
            // Signal shutdown to prevent snapshot from being completed
            putShutdownMetadata(nodeIds);
            createSnapshot(repositoryName, snapshotName, false);
            assertRunningSnapshot(repositoryName, snapshotName);
        } else {
            if (isUpgradedCluster()) {
                deleteShutdownMetadata(nodeIds);
                assertNoShutdownMetadata(nodeIds);
                ensureGreen(indexName);
                assertBusy(() -> assertCompletedSnapshot(repositoryName, snapshotName));
            } else {
                assertRunningSnapshot(repositoryName, snapshotName);
            }
        }
    }

    private void putShutdownMetadata(Collection<String> nodeIds) throws IOException {
        for (String nodeId : nodeIds) {
            final Request putShutdownRequest = new Request("PUT", "/_nodes/" + nodeId + "/shutdown");
            putShutdownRequest.setJsonEntity("""
                {
                  "type": "remove",
                  "reason": "test"
                }""");
            client().performRequest(putShutdownRequest);
        }
    }

    private void deleteShutdownMetadata(Collection<String> nodeIds) throws IOException {
        for (String nodeId : nodeIds) {
            final Request request = new Request("DELETE", "/_nodes/" + nodeId + "/shutdown");
            client().performRequest(request);
        }
    }

    private void assertNoShutdownMetadata(Collection<String> nodeIds) throws IOException {
        final ObjectPath responsePath = assertOKAndCreateObjectPath(
            client().performRequest(new Request("GET", "/_nodes/" + Strings.collectionToCommaDelimitedString(nodeIds) + "/shutdown"))
        );
        assertThat(responsePath.evaluate("nodes"), empty());
    }

    private void assertRunningSnapshot(String repositoryName, String snapshotName) throws IOException {
        final Request request = new Request("GET", "/_snapshot/" + repositoryName + "/_current");
        final ObjectPath responsePath = assertOKAndCreateObjectPath(client().performRequest(request));
        assertThat(responsePath.evaluate("total"), equalTo(1));
        assertThat(responsePath.evaluate("snapshots.0.snapshot"), equalTo(snapshotName));
    }

    private void assertCompletedSnapshot(String repositoryName, String snapshotName) throws IOException {
        {
            final Request request = new Request("GET", "/_snapshot/" + repositoryName + "/_current");
            final ObjectPath responsePath = assertOKAndCreateObjectPath(client().performRequest(request));
            assertThat(responsePath.evaluate("total"), equalTo(0));
        }

        {
            final Request request = new Request("GET", "/_snapshot/" + repositoryName + "/" + snapshotName);
            final ObjectPath responsePath = assertOKAndCreateObjectPath(client().performRequest(request));
            assertThat(responsePath.evaluate("total"), equalTo(1));
            assertThat(responsePath.evaluate("snapshots.0.snapshot"), equalTo(snapshotName));
            assertThat(responsePath.evaluate("snapshots.0.state"), not(equalTo("IN_PROGRESS")));
        }
    }
}
