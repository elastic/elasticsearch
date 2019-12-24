/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ALL;
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

    public void testRejectsInvalidSetting() {
        expectThrows(IllegalArgumentException.class, () ->
            createService(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "unknown").build()));
    }

    public void testSettingCanBeUpdated() {
        createService(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "all").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_ALL));

        clusterSettings.applySettings(Settings.builder().put(NO_MASTER_BLOCK_SETTING.getKey(), "write").build());
        assertThat(noMasterBlockService.getNoMasterBlock(), sameInstance(NO_MASTER_BLOCK_WRITES));
    }
}
