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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

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
        IndexMetadata hiddenIndexMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(false)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false))
            .build();

        assertSystemUpgradeAppliesHiddenSetting(hiddenIndexMetadata);
    }

    /**
     * If a system index erroneously is set to visible, we should remedy that situation.
     */
    public void testHiddenSettingRemovedFromSystemIndices() throws Exception {
        // create an initial cluster state with a hidden index that matches the system index descriptor
        IndexMetadata hiddenIndexMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false))
            .build();

        assertSystemUpgradeAppliesHiddenSetting(hiddenIndexMetadata);
    }

    public void testUpgradeIndexWithVisibleAlias() throws Exception {
        IndexMetadata visibleAliasMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(false)
            .settings(getSettingsBuilder())
            .putAlias(AliasMetadata.builder(SYSTEM_ALIAS_NAME).isHidden(false))
            .build();

        assertSystemUpgradeHidesAlias(visibleAliasMetadata);
    }

    public void testSystemAliasesBecomeHidden() throws Exception {
        IndexMetadata visibleAliasMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder())
            .putAlias(AliasMetadata.builder(SYSTEM_ALIAS_NAME).isHidden(false))
            .build();

        assertSystemUpgradeHidesAlias(visibleAliasMetadata);
    }

    public void testHasVisibleAliases() {
        IndexMetadata nonSystemHiddenAlias = IndexMetadata.builder("user-index")
            .system(false)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .putAlias(AliasMetadata.builder("user-alias").isHidden(true).build())
            .build();
        IndexMetadata nonSystemVisibleAlias = IndexMetadata.builder("user-index")
            .system(false)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .putAlias(AliasMetadata.builder("user-alias").isHidden(false).build())
            .build();
        IndexMetadata systemHiddenAlias = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .putAlias(AliasMetadata.builder(SYSTEM_ALIAS_NAME).isHidden(true).build())
            .build();
        IndexMetadata systemVisibleAlias = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .putAlias(AliasMetadata.builder(SYSTEM_ALIAS_NAME).isHidden(false).build())
            .build();

        // non-system indices should not require update
        assertThat(SystemIndexMetadataUpgradeService.hasVisibleAlias(nonSystemHiddenAlias), equalTo(false));
        assertThat(service.requiresUpdate(nonSystemHiddenAlias), equalTo(false));
        assertThat(SystemIndexMetadataUpgradeService.hasVisibleAlias(nonSystemVisibleAlias), equalTo(true));
        assertThat(service.requiresUpdate(nonSystemVisibleAlias), equalTo(false));

        // hidden system alias should not require update
        assertThat(SystemIndexMetadataUpgradeService.hasVisibleAlias(systemHiddenAlias), equalTo(false));
        assertThat(service.requiresUpdate(systemHiddenAlias), equalTo(false));

        // visible system alias should require update
        assertThat(SystemIndexMetadataUpgradeService.hasVisibleAlias(systemVisibleAlias), equalTo(true));
        assertThat(service.requiresUpdate(systemVisibleAlias), equalTo(true));
    }

    public void testShouldBeSystem() {
        IndexMetadata shouldNotBeSystem = IndexMetadata.builder("user-index")
            .system(true)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .build();
        IndexMetadata shouldBeSystem = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(false)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .build();
        IndexMetadata correctlySystem = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .build();
        IndexMetadata correctlyNonSystem = IndexMetadata.builder("user-index")
            .system(false)
            .settings(getSettingsBuilder().put(SystemIndexDescriptor.DEFAULT_SETTINGS))
            .build();

        // indices that should be toggled from system to non-system
        assertThat(service.shouldBeSystem(shouldNotBeSystem), equalTo(false));
        assertThat(service.requiresUpdate(shouldNotBeSystem), equalTo(true));

        assertThat(service.shouldBeSystem(shouldBeSystem), equalTo(true));
        assertThat(service.requiresUpdate(shouldBeSystem), equalTo(true));

        // indices whose system flag needs no update
        assertThat(service.shouldBeSystem(correctlySystem), equalTo(true));
        assertThat(service.requiresUpdate(correctlySystem), equalTo(false));

        assertThat(service.shouldBeSystem(correctlyNonSystem), equalTo(false));
        assertThat(service.requiresUpdate(correctlyNonSystem), equalTo(false));
    }

    public void testIsVisible() {
        IndexMetadata nonSystemHiddenIndex = IndexMetadata.builder("user-index")
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, "true"))
            .build();
        IndexMetadata nonSystemVisibleIndex = IndexMetadata.builder("user-index")
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, "false"))
            .build();
        IndexMetadata systemHiddenIndex = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, "true"))
            .build();
        IndexMetadata systemVisibleIndex = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .system(true)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, "false"))
            .build();

        // non-system indices should not require update
        assertThat(SystemIndexMetadataUpgradeService.isVisible(nonSystemHiddenIndex), equalTo(false));
        assertThat(service.requiresUpdate(nonSystemHiddenIndex), equalTo(false));
        assertThat(SystemIndexMetadataUpgradeService.isVisible(nonSystemVisibleIndex), equalTo(true));
        assertThat(service.requiresUpdate(nonSystemVisibleIndex), equalTo(false));

        // hidden system index should not require update
        assertThat(SystemIndexMetadataUpgradeService.isVisible(systemHiddenIndex), equalTo(false));
        assertThat(service.requiresUpdate(systemHiddenIndex), equalTo(false));

        // visible system index should require update
        assertThat(SystemIndexMetadataUpgradeService.isVisible(systemVisibleIndex), equalTo(true));
        assertThat(service.requiresUpdate(systemVisibleIndex), equalTo(true));
    }

    private void assertSystemUpgradeAppliesHiddenSetting(IndexMetadata hiddenIndexMetadata) throws Exception {
        assertTrue("Metadata should require update but does not", service.requiresUpdate(hiddenIndexMetadata));
        Metadata.Builder clusterMetadata = new Metadata.Builder();
        clusterMetadata.put(IndexMetadata.builder(hiddenIndexMetadata));

        ClusterState clusterState = ClusterState.builder(new ClusterName("system-index-metadata-upgrade-service-tests"))
            .metadata(clusterMetadata.build())
            .customs(Map.of())
            .build();

        // Get a metadata upgrade task and execute it on the initial cluster state
        ClusterState newState = service.getTask().execute(clusterState);

        IndexMetadata result = newState.metadata().index(SYSTEM_INDEX_NAME);
        assertThat(result.isSystem(), equalTo(true));
        assertThat(result.isHidden(), equalTo(true));
    }

    private void assertSystemUpgradeHidesAlias(IndexMetadata visibleAliasMetadata) throws Exception {
        assertTrue("Metadata should require update but does not", service.requiresUpdate(visibleAliasMetadata));
        Metadata.Builder clusterMetadata = new Metadata.Builder();
        clusterMetadata.put(IndexMetadata.builder(visibleAliasMetadata));

        ClusterState clusterState = ClusterState.builder(new ClusterName("system-index-metadata-upgrade-service-tests"))
            .metadata(clusterMetadata.build())
            .customs(Map.of())
            .build();

        // Get a metadata upgrade task and execute it on the initial cluster state
        ClusterState newState = service.getTask().execute(clusterState);

        IndexMetadata result = newState.metadata().index(SYSTEM_INDEX_NAME);
        assertThat(result.isSystem(), equalTo(true));
        assertThat(result.getAliases().values().stream().allMatch(AliasMetadata::isHidden), equalTo(true));
    }

    private static Settings.Builder getSettingsBuilder() {
        return indexSettings(Version.CURRENT, 1, 0);
    }
}
