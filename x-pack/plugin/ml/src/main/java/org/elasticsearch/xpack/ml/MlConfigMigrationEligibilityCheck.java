/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;

/**
 * Checks whether migration can start and whether ML resources (e.g. jobs, datafeeds)
 * are eligible to be migrated from the cluster state into the config index
 */
public class MlConfigMigrationEligibilityCheck {

    public static final Setting<Boolean> ENABLE_CONFIG_MIGRATION = Setting.boolSetting(
        "xpack.ml.enable_config_migration", true, Setting.Property.Dynamic, Setting.Property.NodeScope);

    private volatile boolean isConfigMigrationEnabled;

    public MlConfigMigrationEligibilityCheck(Settings settings, ClusterService clusterService) {
        isConfigMigrationEnabled = ENABLE_CONFIG_MIGRATION.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLE_CONFIG_MIGRATION, this::setConfigMigrationEnabled);
    }

    private void setConfigMigrationEnabled(boolean configMigrationEnabled) {
        this.isConfigMigrationEnabled = configMigrationEnabled;
    }


    /**
     * Can migration start? Returns:
     *     False if config migration is disabled via the setting {@link #ENABLE_CONFIG_MIGRATION}
     *     False if the .ml-config index shards are not active
     *     True otherwise
     * @param clusterState The cluster state
     * @return A boolean that dictates if config migration can start
     */
    public boolean canStartMigration(ClusterState clusterState) {
        if (isConfigMigrationEnabled == false) {
            return false;
        }
        return mlConfigIndexIsAllocated(clusterState);
    }

    static boolean mlConfigIndexIsAllocated(ClusterState clusterState) {
        if (clusterState.metadata().hasIndex(AnomalyDetectorsIndex.configIndexName()) == false) {
            return false;
        }

        IndexRoutingTable routingTable = clusterState.getRoutingTable().index(AnomalyDetectorsIndex.configIndexName());
        if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
            return false;
        }
        return true;
    }

    /**
     * Is the job a eligible for migration? Returns:
     *     False if {@link #canStartMigration(ClusterState)} returns {@code false}
     *     False if the job is not in the cluster state
     *     False if the {@link Job#isDeleting()}
     *     False if the job has an allocated persistent task
     *     True otherwise i.e. the job is present, not deleting
     *     and does not have a persistent task or its persistent
     *     task is un-allocated
     *
     * @param jobId         The job Id
     * @param clusterState  The cluster state
     * @return A boolean depending on the conditions listed above
     */
    public boolean jobIsEligibleForMigration(String jobId, ClusterState clusterState) {
        if (canStartMigration(clusterState) == false) {
            return false;
        }

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        Job job = mlMetadata.getJobs().get(jobId);

        if (job == null || job.isDeleting()) {
            return false;
        }

        PersistentTasksCustomMetadata persistentTasks = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        return MlTasks.openJobIds(persistentTasks).contains(jobId) == false ||
                MlTasks.unassignedJobIds(persistentTasks, clusterState.nodes()).contains(jobId);
    }

    /**
     * Is the datafeed a eligible for migration? Returns:
     *     False if {@link #canStartMigration(ClusterState)} returns {@code false}
     *     False if the datafeed is not in the cluster state
     *     False if the datafeed has an allocated persistent task
     *     True otherwise i.e. the datafeed is present and does not have a persistent
     *     task or its persistent task is un-allocated
     *
     * @param datafeedId   The datafeed Id
     * @param clusterState  The cluster state
     * @return A boolean depending on the conditions listed above
     */
    public boolean datafeedIsEligibleForMigration(String datafeedId, ClusterState clusterState) {
        if (canStartMigration(clusterState) == false) {
            return false;
        }

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        if (mlMetadata.getDatafeeds().containsKey(datafeedId) == false) {
            return false;
        }

        PersistentTasksCustomMetadata persistentTasks = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        return MlTasks.startedDatafeedIds(persistentTasks).contains(datafeedId) == false
                || MlTasks.unassignedDatafeedIds(persistentTasks, clusterState.nodes()).contains(datafeedId);
    }
}
