/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MaintenanceModeUpdateTaskTests extends ESTestCase {

    public void testExecute() {
        assertMove(OperationMode.NORMAL, OperationMode.MAINTENANCE_REQUESTED);
        assertMove(OperationMode.MAINTENANCE_REQUESTED, randomFrom(OperationMode.NORMAL, OperationMode.MAINTENANCE));
        assertMove(OperationMode.MAINTENANCE, OperationMode.NORMAL);

        OperationMode mode = randomFrom(OperationMode.values());
        assertNoMove(mode, mode);
        assertNoMove(OperationMode.MAINTENANCE, OperationMode.MAINTENANCE_REQUESTED);
        assertNoMove(OperationMode.NORMAL, OperationMode.MAINTENANCE);
    }

    public void testExecuteWithEmptyMetadata() {
        OperationMode requestedMode = OperationMode.MAINTENANCE_REQUESTED;
        OperationMode newMode = executeUpdate(false, IndexLifecycleMetadata.EMPTY.getMaintenanceMode(),
            requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));

        requestedMode = randomFrom(OperationMode.NORMAL, OperationMode.MAINTENANCE);
        newMode = executeUpdate(false, IndexLifecycleMetadata.EMPTY.getMaintenanceMode(),
            requestedMode, false);
        assertThat(newMode, equalTo(OperationMode.NORMAL));
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
        ImmutableOpenMap.Builder<String, MetaData.Custom> customsMapBuilder = ImmutableOpenMap.builder();
        MetaData.Builder metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build());
        if (metadataInstalled) {
            metaData.customs(customsMapBuilder.fPut(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata).build());
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        MaintenanceModeUpdateTask task = new MaintenanceModeUpdateTask(requestMode);
        ClusterState newState = task.execute(state);
        if (assertSameClusterState) {
            assertSame(state, newState);
        } else {
            assertThat(state, not(equalTo(newState)));
        }
        IndexLifecycleMetadata newMetaData = newState.metaData().custom(IndexLifecycleMetadata.TYPE);
        assertThat(newMetaData.getPolicyMetadatas(), equalTo(indexLifecycleMetadata.getPolicyMetadatas()));
        return newMetaData.getMaintenanceMode();
    }
}
