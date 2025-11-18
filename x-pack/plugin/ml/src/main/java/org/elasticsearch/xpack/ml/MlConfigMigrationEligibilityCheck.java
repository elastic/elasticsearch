/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;

/**
 * Checks whether ML config migration can start.
 *
 * Originally this class was used to check whether jobs and datafeeds could be
 * migrated from cluster state to the ML config index. This use case is no longer
 * relevant. However, it's possible that in the future we may want to do some
 * other form of config migration, and the cluster setting defined in this class
 * would be an ideal way to restrict it.
 */
public class MlConfigMigrationEligibilityCheck {

    public static final Setting<Boolean> ENABLE_CONFIG_MIGRATION = Setting.boolSetting(
        "xpack.ml.enable_config_migration",
        true,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

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
        IndexAbstraction configIndexOrAlias = clusterState.metadata().getProject().getIndicesLookup().get(MlConfigIndex.indexName());
        if (configIndexOrAlias == null) {
            return false;
        }

        IndexRoutingTable routingTable = clusterState.getRoutingTable().index(configIndexOrAlias.getWriteIndex());
        return routingTable != null && routingTable.allPrimaryShardsActive() && routingTable.readyForSearch();
    }
}
