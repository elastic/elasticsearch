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

public class MaintenanceModeUpdateTaskTests extends ESTestCase {

    public void testExecute() {
        // assert can change out of OUT
        OperationMode currentMode = OperationMode.NORMAL;
        OperationMode requestedMode = randomValueOtherThan(currentMode, () -> randomFrom(OperationMode.values()));
        OperationMode newMode = executeUpdate(currentMode, requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));

        currentMode = OperationMode.MAINTENANCE_REQUESTED;
        requestedMode = randomValueOtherThan(currentMode, () -> randomFrom(OperationMode.values()));
        newMode = executeUpdate(currentMode, requestedMode, false);
        assertThat(newMode, equalTo(requestedMode));

        // assert no change when changing to the same mode
        currentMode = randomFrom(OperationMode.values());
        requestedMode = currentMode;
        newMode = executeUpdate(currentMode, currentMode, true);
        assertThat(newMode, equalTo(requestedMode));


        // assert no change when requesting to be put in maintenance-mode, but already in it
        newMode = executeUpdate(OperationMode.MAINTENANCE, OperationMode.MAINTENANCE_REQUESTED, true);
        assertThat(newMode, equalTo(OperationMode.MAINTENANCE));
    }

    private OperationMode executeUpdate(OperationMode currentMode, OperationMode requestMode, boolean assertSameClusterState) {
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(Collections.emptyMap(), currentMode);
        ImmutableOpenMap.Builder<String, MetaData.Custom> customsMapBuilder = ImmutableOpenMap.builder();
        MetaData metaData = MetaData.builder()
            .customs(customsMapBuilder.fPut(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata).build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        MaintenanceModeUpdateTask task = new MaintenanceModeUpdateTask(requestMode);
        ClusterState newState = task.execute(state);
        if (assertSameClusterState) {
            assertSame(state, newState);
        }
        IndexLifecycleMetadata newMetaData = newState.metaData().custom(IndexLifecycleMetadata.TYPE);
        assertThat(newMetaData.getPolicyMetadatas(), equalTo(indexLifecycleMetadata.getPolicyMetadatas()));
        return newMetaData.getMaintenanceMode();
    }
}
