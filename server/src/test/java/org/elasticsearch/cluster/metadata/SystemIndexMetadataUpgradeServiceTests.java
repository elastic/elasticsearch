/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class SystemIndexMetadataUpgradeServiceTests extends ESTestCase {

    private static final String MAPPINGS = "{ \"_doc\": { \"_meta\": { \"version\": \"7.4.0\" } } }";
    private static final String SYSTEM_INDEX_NAME = ".myindex-1";
    private static final String SYSTEM_ALIAS_NAME = ".myindex-alias";
    private static final SystemIndexDescriptor DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".myindex-*")
        .setPrimaryIndex(SYSTEM_INDEX_NAME)
        .setAliasName(SYSTEM_ALIAS_NAME)
        .setSettings(getSettingsBuilder().build())
        .setMappings(MAPPINGS)
        .setVersionMetaKey("version")
        .setOrigin("FAKE_ORIGIN")
        .build();

    private SystemIndexMetadataUpgradeService service;

    @Before
    public void setUpTest() {
        // set up a system index upgrade service
        this.service = new SystemIndexMetadataUpgradeService(
            new SystemIndices(List.of(new SystemIndices.Feature("foo", "a test feature", List.of(DESCRIPTOR)))),
            mock(ClusterService.class)
        );
    }

    /**
     * When we upgrade Elasticsearch versions, existing indices may be newly
     * defined as system indices. If such indices are set without "hidden," we need
     * to update that setting.
     */
    public void testUpgradeVisibleIndexToSystemIndex() throws Exception {
        // create an initial cluster state with a hidden index that matches the system index descriptor
        IndexMetadata.Builder hiddenIndexMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(false)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false));

        assertSystemUpgradeAppliesHiddenSetting(hiddenIndexMetadata);
    }

    /**
     * If a system index erroneously is set to visible, we should remedy that situation.
     */
    public void testHiddenSettingRemovedFromSystemIndices() throws Exception {
        // create an initial cluster state with a hidden index that matches the system index descriptor
        IndexMetadata.Builder hiddenIndexMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false));

        assertSystemUpgradeAppliesHiddenSetting(hiddenIndexMetadata);
    }

    public void testUpgradeIndexWithVisibleAlias() throws Exception {
        IndexMetadata.Builder visibleAliasMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(false)
            .settings(getSettingsBuilder())
            .putAlias(AliasMetadata.builder(SYSTEM_ALIAS_NAME).isHidden(false));

        assertSystemUpgradeHidesAlias(visibleAliasMetadata);
    }

    public void testSystemAliasesBecomeHidden() throws Exception {
        IndexMetadata.Builder visibleAliasMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder())
            .putAlias(AliasMetadata.builder(SYSTEM_ALIAS_NAME).isHidden(false));

        assertSystemUpgradeHidesAlias(visibleAliasMetadata);
    }

    private void assertSystemUpgradeAppliesHiddenSetting(IndexMetadata.Builder hiddenIndexMetadata) throws Exception {
        Metadata.Builder clusterMetadata = new Metadata.Builder();
        clusterMetadata.put(hiddenIndexMetadata);

        ClusterState clusterState = ClusterState.builder(new ClusterName("system-index-metadata-upgrade-service-tests"))
            .metadata(clusterMetadata.build())
            .customs(ImmutableOpenMap.of())
            .build();

        // Get a metadata upgrade task and execute it on the initial cluster state
        ClusterState newState = service.getTask().execute(clusterState);

        IndexMetadata result = newState.metadata().index(SYSTEM_INDEX_NAME);
        assertThat(result.isSystem(), equalTo(true));
        assertThat(result.isHidden(), equalTo(true));
    }

    private void assertSystemUpgradeHidesAlias(IndexMetadata.Builder visibleAliasMetadata) throws Exception {
        Metadata.Builder clusterMetadata = new Metadata.Builder();
        clusterMetadata.put(visibleAliasMetadata);

        ClusterState clusterState = ClusterState.builder(new ClusterName("system-index-metadata-upgrade-service-tests"))
            .metadata(clusterMetadata.build())
            .customs(ImmutableOpenMap.of())
            .build();

        // Get a metadata upgrade task and execute it on the initial cluster state
        ClusterState newState = service.getTask().execute(clusterState);

        IndexMetadata result = newState.metadata().index(SYSTEM_INDEX_NAME);
        assertThat(result.isSystem(), equalTo(true));
        assertThat(result.getAliases().values().stream().allMatch(AliasMetadata::isHidden), equalTo(true));
    }

    private static Settings.Builder getSettingsBuilder() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
    }
}
