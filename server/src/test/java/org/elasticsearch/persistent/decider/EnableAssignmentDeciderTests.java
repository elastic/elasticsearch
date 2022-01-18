/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent.decider;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksDecidersTestCase;

public class EnableAssignmentDeciderTests extends PersistentTasksDecidersTestCase {

    public void testAllocationValues() {
        final String all = randomFrom("all", "All", "ALL");
        assertEquals(EnableAssignmentDecider.Allocation.ALL, EnableAssignmentDecider.Allocation.fromString(all));

        final String none = randomFrom("none", "None", "NONE");
        assertEquals(EnableAssignmentDecider.Allocation.NONE, EnableAssignmentDecider.Allocation.fromString(none));
    }

    public void testEnableAssignment() {
        final int nbTasks = randomIntBetween(1, 10);
        final int nbNodes = randomIntBetween(1, 5);
        final EnableAssignmentDecider.Allocation allocation = randomFrom(EnableAssignmentDecider.Allocation.values());

        Settings settings = Settings.builder()
            .put(EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), allocation.toString())
            .build();
        updateSettings(settings);

        ClusterState clusterState = reassign(createClusterStateWithTasks(nbNodes, nbTasks));
        if (allocation == EnableAssignmentDecider.Allocation.ALL) {
            assertNbAssignedTasks(nbTasks, clusterState);
        } else {
            assertNbUnassignedTasks(nbTasks, clusterState);
        }
    }
}
