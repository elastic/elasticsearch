/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.desirednodes.GetDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesResponse;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DesiredNodesSnapshotsIT extends AbstractSnapshotIntegTestCase {
    public void testDesiredNodesAreNotIncludedInSnapshotsClusterState() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        updateDesiredNodes(updateDesiredNodesRequest);

        final var indexName = "idx";
        createIndex(indexName);

        final var repositoryName = "my-repo";
        createRepository(repositoryName, "fs");
        final var snapshotName = "snapshot";
        createFullSnapshot(repositoryName, snapshotName);

        indicesAdmin().prepareDelete(indexName).get();

        final var updateDesiredNodesWithNewHistoryRequest = randomUpdateDesiredNodesRequest();
        final var updateDesiredNodesResponse = updateDesiredNodes(updateDesiredNodesWithNewHistoryRequest);
        assertThat(updateDesiredNodesResponse.hasReplacedExistingHistoryId(), is(true));

        final var desiredNodesAfterSnapshot = getLatestDesiredNodes();

        clusterAdmin().prepareRestoreSnapshot(repositoryName, snapshotName).setRestoreGlobalState(true).get();

        final var desiredNodesAfterRestore = getLatestDesiredNodes();
        assertThat(desiredNodesAfterRestore.historyID(), is(equalTo(desiredNodesAfterSnapshot.historyID())));
        assertThat(desiredNodesAfterRestore.version(), is(equalTo(desiredNodesAfterSnapshot.version())));
    }

    private UpdateDesiredNodesResponse updateDesiredNodes(UpdateDesiredNodesRequest updateDesiredNodesRequest) {
        return client().execute(UpdateDesiredNodesAction.INSTANCE, updateDesiredNodesRequest).actionGet();
    }

    private DesiredNodes getLatestDesiredNodes() {
        return client().execute(GetDesiredNodesAction.INSTANCE, new GetDesiredNodesAction.Request()).actionGet().getDesiredNodes();
    }

    private UpdateDesiredNodesRequest randomUpdateDesiredNodesRequest() {
        return new UpdateDesiredNodesRequest(
            randomAlphaOfLength(10),
            randomIntBetween(1, 10),
            randomList(
                1,
                10,
                () -> new DesiredNode(
                    Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build(),
                    randomIntBetween(1, 10),
                    ByteSizeValue.ofGb(randomIntBetween(16, 64)),
                    ByteSizeValue.ofGb(randomIntBetween(128, 256)),
                    Version.CURRENT
                )
            ),
            false
        );
    }
}
