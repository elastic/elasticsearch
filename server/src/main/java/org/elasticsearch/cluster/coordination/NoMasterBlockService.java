/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;

public class NoMasterBlockService {
    public static final int NO_MASTER_BLOCK_ID = 2;
    public static final ClusterBlock NO_MASTER_BLOCK_WRITES = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, false, false,
        RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));
    public static final ClusterBlock NO_MASTER_BLOCK_ALL = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, true, false,
        RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);
    public static final ClusterBlock NO_MASTER_BLOCK_METADATA_WRITES = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, false, false,
        RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.METADATA_WRITE));

    public static final Setting<ClusterBlock> NO_MASTER_BLOCK_SETTING =
        new Setting<>("cluster.no_master_block", "write", NoMasterBlockService::parseNoMasterBlock,
            Property.Dynamic, Property.NodeScope);

    private volatile ClusterBlock noMasterBlock;

    public NoMasterBlockService(Settings settings, ClusterSettings clusterSettings) {
        this.noMasterBlock = NO_MASTER_BLOCK_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(NO_MASTER_BLOCK_SETTING, this::setNoMasterBlock);
    }

    private static ClusterBlock parseNoMasterBlock(String value) {
        switch (value) {
            case "all":
                return NO_MASTER_BLOCK_ALL;
            case "write":
                return NO_MASTER_BLOCK_WRITES;
            case "metadata_write":
                return NO_MASTER_BLOCK_METADATA_WRITES;
            default:
                throw new IllegalArgumentException("invalid no-master block [" + value + "], must be one of [all, write, metadata_write]");
        }
    }

    public ClusterBlock getNoMasterBlock() {
        return noMasterBlock;
    }

    private void setNoMasterBlock(ClusterBlock noMasterBlock) {
        this.noMasterBlock = noMasterBlock;
    }
}
