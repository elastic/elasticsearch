/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.snapshot.upgrader;

import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SnapshotUpgradePredicateTests extends ESTestCase {

    public void testWhenWaitForCompletionIsTrue() {
        final PersistentTask<SnapshotUpgradeTaskParams> assignedTask = new PersistentTask<>(
            "task_id",
            MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("job", "snapshot"),
            1,
            new PersistentTasksCustomMetadata.Assignment("test-node", "")
        );
        {
            SnapshotUpgradePredicate snapshotUpgradePredicate = new SnapshotUpgradePredicate(true, logger);
            assertThat(snapshotUpgradePredicate.test(null), is(true));
            assertThat(snapshotUpgradePredicate.isCompleted(), is(true));
        }

        {
            SnapshotUpgradePredicate snapshotUpgradePredicate = new SnapshotUpgradePredicate(true, logger);
            assertThat(snapshotUpgradePredicate.test(assignedTask), is(false));
            assertThat(snapshotUpgradePredicate.isCompleted(), is(false));
        }

        {
            PersistentTask<SnapshotUpgradeTaskParams> failedAssignedTask = new PersistentTask<>(
                assignedTask,
                new SnapshotUpgradeTaskState(SnapshotUpgradeState.FAILED, 1, "this reason")
            );
            SnapshotUpgradePredicate snapshotUpgradePredicate = new SnapshotUpgradePredicate(true, logger);
            assertThat(snapshotUpgradePredicate.test(failedAssignedTask), is(true));
            assertThat(snapshotUpgradePredicate.isCompleted(), is(false));
            assertThat(snapshotUpgradePredicate.isShouldCancel(), is(true));
            assertThat(snapshotUpgradePredicate.getException(), is(notNullValue()));
            assertThat(
                snapshotUpgradePredicate.getException().getMessage(),
                containsString("while waiting for to be assigned to a node; recorded reason [this reason]")
            );
        }

    }

    public void testWhenWaitForCompletionIsFalse() {
        final PersistentTask<SnapshotUpgradeTaskParams> assignedTask = new PersistentTask<>(
            "task_id",
            MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("job", "snapshot"),
            1,
            new PersistentTasksCustomMetadata.Assignment("test-node", "")
        );
        {
            SnapshotUpgradePredicate snapshotUpgradePredicate = new SnapshotUpgradePredicate(false, logger);
            assertThat(snapshotUpgradePredicate.test(null), is(true));
            assertThat(snapshotUpgradePredicate.isCompleted(), is(true));
        }

        {
            SnapshotUpgradePredicate snapshotUpgradePredicate = new SnapshotUpgradePredicate(false, logger);
            assertThat(snapshotUpgradePredicate.test(assignedTask), is(true));
            assertThat(snapshotUpgradePredicate.isCompleted(), is(false));
        }

        {
            PersistentTask<SnapshotUpgradeTaskParams> failedAssignedTask = new PersistentTask<>(
                assignedTask,
                new SnapshotUpgradeTaskState(SnapshotUpgradeState.FAILED, 1, "this reason")
            );
            SnapshotUpgradePredicate snapshotUpgradePredicate = new SnapshotUpgradePredicate(false, logger);
            assertThat(snapshotUpgradePredicate.test(failedAssignedTask), is(true));
            assertThat(snapshotUpgradePredicate.isCompleted(), is(false));
            assertThat(snapshotUpgradePredicate.isShouldCancel(), is(true));
            assertThat(snapshotUpgradePredicate.getException(), is(notNullValue()));
            assertThat(
                snapshotUpgradePredicate.getException().getMessage(),
                containsString("while waiting for to be assigned to a node; recorded reason [this reason]")
            );
        }

    }
}
