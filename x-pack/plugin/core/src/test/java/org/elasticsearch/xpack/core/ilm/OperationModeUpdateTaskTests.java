/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class OperationModeUpdateTaskTests extends ESTestCase {

    public void testILMExecute() {
        assertILMMove(OperationMode.RUNNING, OperationMode.STOPPING);
        assertILMMove(OperationMode.STOPPING, OperationMode.RUNNING);
        assertILMMove(OperationMode.STOPPING, OperationMode.STOPPED);
        assertILMMove(OperationMode.STOPPED, OperationMode.RUNNING);

        OperationMode mode = randomFrom(OperationMode.values());
        assertNoILMMove(mode, mode);
        assertNoILMMove(OperationMode.STOPPED, OperationMode.STOPPING);
        assertNoILMMove(OperationMode.RUNNING, OperationMode.STOPPED);
    }

    public void testSLMExecute() {
        assertSLMMove(OperationMode.RUNNING, OperationMode.STOPPING);
        assertSLMMove(OperationMode.STOPPING, OperationMode.RUNNING);
        assertSLMMove(OperationMode.STOPPING, OperationMode.STOPPED);
        assertSLMMove(OperationMode.STOPPED, OperationMode.RUNNING);

        OperationMode mode = randomFrom(OperationMode.values());
        assertNoSLMMove(mode, mode);
        assertNoSLMMove(OperationMode.STOPPED, OperationMode.STOPPING);
        assertNoSLMMove(OperationMode.RUNNING, OperationMode.STOPPED);
    }

    public void testExecuteWithEmptyMetadata() {
        OperationMode requestedMode = OperationMode.STOPPING;
        OperationMode newMode = executeILMUpdate(false, LifecycleOperationMetadata.EMPTY.getILMOperationMode(), requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));

        newMode = executeSLMUpdate(false, LifecycleOperationMetadata.EMPTY.getSLMOperationMode(), requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));

        requestedMode = OperationMode.RUNNING;
        newMode = executeILMUpdate(false, LifecycleOperationMetadata.EMPTY.getILMOperationMode(), requestedMode, true);
        assertThat(newMode, equalTo(OperationMode.RUNNING));
        requestedMode = OperationMode.STOPPED;
        newMode = executeILMUpdate(false, LifecycleOperationMetadata.EMPTY.getILMOperationMode(), requestedMode, true);
        assertThat(newMode, equalTo(OperationMode.RUNNING));

        requestedMode = OperationMode.RUNNING;
        newMode = executeSLMUpdate(false, LifecycleOperationMetadata.EMPTY.getSLMOperationMode(), requestedMode, true);
        assertThat(newMode, equalTo(OperationMode.RUNNING));
        requestedMode = OperationMode.STOPPED;
        newMode = executeSLMUpdate(false, LifecycleOperationMetadata.EMPTY.getSLMOperationMode(), requestedMode, true);
        assertThat(newMode, equalTo(OperationMode.RUNNING));
    }

    private void assertILMMove(OperationMode currentMode, OperationMode requestedMode) {
        OperationMode newMode = executeILMUpdate(true, currentMode, requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));
    }

    private void assertSLMMove(OperationMode currentMode, OperationMode requestedMode) {
        OperationMode newMode = executeSLMUpdate(true, currentMode, requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));
    }

    private void assertNoILMMove(OperationMode currentMode, OperationMode requestedMode) {
        OperationMode newMode = executeILMUpdate(true, currentMode, requestedMode, true);
        assertThat(newMode, equalTo(currentMode));
    }

    private void assertNoSLMMove(OperationMode currentMode, OperationMode requestedMode) {
        OperationMode newMode = executeSLMUpdate(true, currentMode, requestedMode, true);
        assertThat(newMode, equalTo(currentMode));
    }

    @SuppressWarnings("deprecated")
    private OperationMode executeILMUpdate(
        boolean metadataInstalled,
        OperationMode currentMode,
        OperationMode requestMode,
        boolean assertSameClusterState
    ) {
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(Map.of(), currentMode);
        SnapshotLifecycleMetadata snapshotLifecycleMetadata = new SnapshotLifecycleMetadata(
            Map.of(),
            currentMode,
            new SnapshotLifecycleStats()
        );
        Metadata.Builder metadata = Metadata.builder().persistentSettings(settings(IndexVersion.current()).build());
        if (metadataInstalled) {
            metadata.projectCustoms(
                Map.of(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata, SnapshotLifecycleMetadata.TYPE, snapshotLifecycleMetadata)
            );
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        OperationModeUpdateTask task = OperationModeUpdateTask.ilmMode(requestMode);
        ClusterState newState = task.execute(state);
        if (assertSameClusterState) {
            assertSame("expected the same state instance but they were different", state, newState);
        } else {
            assertThat("expected a different state instance but they were the same", state, not(equalTo(newState)));
        }
        LifecycleOperationMetadata newMetadata = newState.metadata().getProject().custom(LifecycleOperationMetadata.TYPE);
        IndexLifecycleMetadata oldMetadata = newState.metadata()
            .getProject()
            .custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        return Optional.ofNullable(newMetadata)
            .map(LifecycleOperationMetadata::getILMOperationMode)
            .orElseGet(oldMetadata::getOperationMode);
    }

    @SuppressWarnings("deprecated")
    private OperationMode executeSLMUpdate(
        boolean metadataInstalled,
        OperationMode currentMode,
        OperationMode requestMode,
        boolean assertSameClusterState
    ) {
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(Map.of(), currentMode);
        SnapshotLifecycleMetadata snapshotLifecycleMetadata = new SnapshotLifecycleMetadata(
            Map.of(),
            currentMode,
            new SnapshotLifecycleStats()
        );
        Metadata.Builder metadata = Metadata.builder().persistentSettings(settings(IndexVersion.current()).build());
        if (metadataInstalled) {
            metadata.projectCustoms(
                Map.of(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata, SnapshotLifecycleMetadata.TYPE, snapshotLifecycleMetadata)
            );
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        OperationModeUpdateTask task = OperationModeUpdateTask.slmMode(requestMode);
        ClusterState newState = task.execute(state);
        if (assertSameClusterState) {
            assertSame(state, newState);
        } else {
            assertThat(state, not(equalTo(newState)));
        }
        LifecycleOperationMetadata newMetadata = newState.metadata().getProject().custom(LifecycleOperationMetadata.TYPE);
        SnapshotLifecycleMetadata oldMetadata = newState.metadata()
            .getProject()
            .custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
        return Optional.ofNullable(newMetadata)
            .map(LifecycleOperationMetadata::getSLMOperationMode)
            .orElseGet(oldMetadata::getOperationMode);
    }
}
