/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SystemIndexMetadataUpgradeServiceTests extends ESTestCase {

    private static final String MAPPINGS = String.format(Locale.ROOT, """
        {
          "_doc": {
            "_meta": {
              "version": "7.4.0",
              "%s": 0
            }
          }
        }
        """, SystemIndexDescriptor.VERSION_META_KEY);
    private static final String SYSTEM_INDEX_NAME = ".myindex-1";
    private static final String SYSTEM_ALIAS_NAME = ".myindex-alias";
    private static final SystemIndexDescriptor DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".myindex-*")
        .setPrimaryIndex(SYSTEM_INDEX_NAME)
        .setAliasName(SYSTEM_ALIAS_NAME)
        .setSettings(getSettingsBuilder().build())
        .setMappings(MAPPINGS)
        .setOrigin("FAKE_ORIGIN")
        .build();

    private static final String SYSTEM_DATA_STREAM_NAME = ".my-ds";
    private static final String SYSTEM_DATA_STREAM_INDEX_NAME = DataStream.BACKING_INDEX_PREFIX + SYSTEM_DATA_STREAM_NAME + "-1";
    private static final String SYSTEM_DATA_STREAM_FAILSTORE_NAME = DataStream.FAILURE_STORE_PREFIX + SYSTEM_DATA_STREAM_NAME;
    private static final SystemDataStreamDescriptor SYSTEM_DATA_STREAM_DESCRIPTOR = new SystemDataStreamDescriptor(
        SYSTEM_DATA_STREAM_NAME,
        "System datastream for test",
        SystemDataStreamDescriptor.Type.INTERNAL,
        ComposableIndexTemplate.builder().build(),
        Collections.emptyMap(),
        Collections.singletonList("FAKE_ORIGIN"),
        "FAKE_ORIGIN",
        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
    );

    private SystemIndexMetadataUpgradeService service;
    private ClusterStateTaskListener task;
    private ClusterStateTaskExecutor<ClusterStateTaskListener> executor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpTest() {
        // set up a system index upgrade service
        ClusterService clusterService = mock(ClusterService.class);
        MasterServiceTaskQueue<ClusterStateTaskListener> queue = mock(MasterServiceTaskQueue.class);
        when(clusterService.createTaskQueue(eq("system-indices-metadata-upgrade"), eq(Priority.NORMAL), any())).thenAnswer(invocation -> {
            executor = invocation.getArgument(2, ClusterStateTaskExecutor.class);
            return queue;
        });
        doAnswer(invocation -> {
            task = invocation.getArgument(1, ClusterStateTaskListener.class);
            return null;
        }).when(queue).submitTask(any(), any(), any());

        this.service = new SystemIndexMetadataUpgradeService(
            new SystemIndices(
                List.of(
                    new SystemIndices.Feature("foo", "a test feature", List.of(DESCRIPTOR)),
                    new SystemIndices.Feature(
                        "sds",
                        "system data stream feature",
                        Collections.emptyList(),
                        Collections.singletonList(SYSTEM_DATA_STREAM_DESCRIPTOR)
                    )
                )
            ),
            clusterService
        );
    }

    private ClusterState executeTask(ClusterState clusterState) {
        try {
            return ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(clusterState, executor, Collections.singletonList(task));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public void testUpgradeDataStreamToSystemDataStream() {
        IndexMetadata dsIndexMetadata = IndexMetadata.builder(SYSTEM_DATA_STREAM_INDEX_NAME)
            .system(false)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true))
            .build();
        IndexMetadata fsIndexMetadata = IndexMetadata.builder(SYSTEM_DATA_STREAM_FAILSTORE_NAME)
            .system(false)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true))
            .build();
        DataStream.DataStreamIndices failureIndices = DataStream.DataStreamIndices.failureIndicesBuilder(
            Collections.singletonList(fsIndexMetadata.getIndex())
        ).build();
        DataStream dataStream = DataStream.builder(SYSTEM_DATA_STREAM_NAME, Collections.singletonList(dsIndexMetadata.getIndex()))
            .setFailureIndices(failureIndices)
            .setHidden(false)
            .setSystem(false)
            .build();

        assertTrue(dataStream.containsIndex(dsIndexMetadata.getIndex().getName()));
        assertTrue(dataStream.containsIndex(fsIndexMetadata.getIndex().getName()));

        Metadata.Builder clusterMetadata = new Metadata.Builder();
        clusterMetadata.put(dataStream);
        clusterMetadata.put(dsIndexMetadata, true);
        clusterMetadata.put(fsIndexMetadata, true);

        ClusterState clusterState = ClusterState.builder(new ClusterName("system-index-metadata-upgrade-service-tests"))
            .metadata(clusterMetadata.build())
            .customs(Map.of())
            .build();

        service.submitUpdateTask(Collections.emptyList(), Collections.singletonList(dataStream));
        // Execute a metadata upgrade task on the initial cluster state
        ClusterState newState = executeTask(clusterState);

        DataStream updatedDataStream = newState.metadata().getProject().dataStreams().get(dataStream.getName());
        assertThat(updatedDataStream.isSystem(), equalTo(true));
        assertThat(updatedDataStream.isHidden(), equalTo(true));

        IndexMetadata updatedIndexMetadata = newState.metadata().getProject().index(dsIndexMetadata.getIndex().getName());
        assertThat(updatedIndexMetadata.isSystem(), equalTo(true));
        assertThat(updatedIndexMetadata.isHidden(), equalTo(true));

        IndexMetadata updatedFailstoreMetadata = newState.metadata().getProject().index(fsIndexMetadata.getIndex().getName());
        assertThat(updatedFailstoreMetadata.isSystem(), equalTo(true));
        assertThat(updatedFailstoreMetadata.isHidden(), equalTo(true));
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

    private void assertSystemUpgradeAppliesHiddenSetting(IndexMetadata hiddenIndexMetadata) {
        assertTrue("Metadata should require update but does not", service.requiresUpdate(hiddenIndexMetadata));
        Metadata.Builder clusterMetadata = new Metadata.Builder();
        clusterMetadata.put(IndexMetadata.builder(hiddenIndexMetadata));

        ClusterState clusterState = ClusterState.builder(new ClusterName("system-index-metadata-upgrade-service-tests"))
            .metadata(clusterMetadata.build())
            .customs(Map.of())
            .build();

        service.submitUpdateTask(Collections.singletonList(hiddenIndexMetadata.getIndex()), Collections.emptyList());
        // Get a metadata upgrade task and execute it on the initial cluster state
        ClusterState newState = executeTask(clusterState);

        IndexMetadata result = newState.metadata().getProject().index(SYSTEM_INDEX_NAME);
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

        service.submitUpdateTask(Collections.singletonList(visibleAliasMetadata.getIndex()), Collections.emptyList());
        // Get a metadata upgrade task and execute it on the initial cluster state
        ClusterState newState = executeTask(clusterState);

        IndexMetadata result = newState.metadata().getProject().index(SYSTEM_INDEX_NAME);
        assertThat(result.isSystem(), equalTo(true));
        assertThat(result.getAliases().values().stream().allMatch(AliasMetadata::isHidden), equalTo(true));
    }

    private static Settings.Builder getSettingsBuilder() {
        return indexSettings(IndexVersion.current(), 1, 0);
    }
}
