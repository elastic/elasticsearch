/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ALL;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_METADATA_WRITES;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_WRITES;
import static org.elasticsearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.hamcrest.Matchers.sameInstance;

public class NoMasterBlockServiceTests extends ESTestCase {

    private NoMasterBlockService noMasterBlockService;
    private ClusterSettings clusterSettings;

    private void createService(Settings settings) {
        clusterSettings = new ClusterSettings(settings, BUILT_IN_CLUSTER_SETTINGS);
        noMasterBlockService = new NoMasterBlockService(settings, clusterSettings);
    }

    public void testBlocksWritesByDefault() {
        createService(Settings.EMPTY);
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_WRITES));
    }

    public void testBlocksWritesIfConfiguredBySetting() {
        createService(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "write").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_WRITES));
    }

    public void testBlocksAllIfConfiguredBySetting() {
        createService(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "all").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_ALL));
    }

    public void testBlocksMetadataWritesIfConfiguredBySetting() {
        createService(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "metadata_write").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_METADATA_WRITES));
    }

    public void testRejectsInvalidSetting() {
        expectThrows(IllegalArgumentException.class, () ->
            createService(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "unknown").build()));
    }

    public void testSettingCanBeUpdated() {
        createService(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "all").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_ALL));

        clusterSettings.applySettings(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "write").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_WRITES));

        clusterSettings.applySettings(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "metadata_write").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_METADATA_WRITES));
    }
}
