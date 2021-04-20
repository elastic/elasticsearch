/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class OperationModeUpdateTaskTests extends ESTestCase {

    public void testExecute() {
        assertMove(OperationMode.RUNNING, OperationMode.STOPPING);
        assertMove(OperationMode.STOPPING, randomFrom(OperationMode.RUNNING, OperationMode.STOPPED));
        assertMove(OperationMode.STOPPED, OperationMode.RUNNING);

        OperationMode mode = randomFrom(OperationMode.values());
        assertNoMove(mode, mode);
        assertNoMove(OperationMode.STOPPED, OperationMode.STOPPING);
        assertNoMove(OperationMode.RUNNING, OperationMode.STOPPED);
    }

    public void testExecuteWithEmptyMetadata() {
        OperationMode requestedMode = OperationMode.STOPPING;
        OperationMode newMode = executeUpdate(false, IndexLifecycleMetadata.EMPTY.getOperationMode(),
            requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));

        requestedMode = randomFrom(OperationMode.RUNNING, OperationMode.STOPPED);
        newMode = executeUpdate(false, IndexLifecycleMetadata.EMPTY.getOperationMode(),
            requestedMode, false);
        assertThat(newMode, equalTo(OperationMode.RUNNING));
    }

    private void assertMove(OperationMode currentMode, OperationMode requestedMode) {
        OperationMode newMode = executeUpdate(true, currentMode, requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));
    }

    private void assertNoMove(OperationMode currentMode, OperationMode requestedMode) {
        OperationMode newMode = executeUpdate(true, currentMode, requestedMode, true);
        assertThat(newMode, equalTo(currentMode));
    }

    private OperationMode executeUpdate(boolean metadataInstalled, OperationMode currentMode, OperationMode requestMode,
                                        boolean assertSameClusterState) {
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(Collections.emptyMap(), currentMode);
        SnapshotLifecycleMetadata snapshotLifecycleMetadata =
            new SnapshotLifecycleMetadata(Collections.emptyMap(), currentMode, new SnapshotLifecycleStats());
        ImmutableOpenMap.Builder<String, Metadata.Custom> customsMapBuilder = ImmutableOpenMap.builder();
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build());
        if (metadataInstalled) {
            metadata.customs(customsMapBuilder
                .fPut(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata)
                .fPut(SnapshotLifecycleMetadata.TYPE, snapshotLifecycleMetadata)
                .build());
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        OperationModeUpdateTask task = OperationModeUpdateTask.ilmMode(requestMode);
        ClusterState newState = task.execute(state);
        if (assertSameClusterState) {
            assertSame(state, newState);
        } else {
            assertThat(state, not(equalTo(newState)));
        }
        IndexLifecycleMetadata newMetadata = newState.metadata().custom(IndexLifecycleMetadata.TYPE);
        assertThat(newMetadata.getPolicyMetadatas(), equalTo(indexLifecycleMetadata.getPolicyMetadatas()));
        return newMetadata.getOperationMode();
    }
}
