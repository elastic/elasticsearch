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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class SystemIndexMetadataUpgradeServiceTests extends ESTestCase {

    private static final String MAPPINGS = "{ \"_doc\": { \"_meta\": { \"version\": \"7.4.0\" } } }";
    private static final String SYSTEM_INDEX_NAME = ".myindex-1";
    private static final SystemIndexDescriptor DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".myindex-*")
        .setPrimaryIndex(SYSTEM_INDEX_NAME)
        .setSettings(getSettingsBuilder().build())
        .setMappings(MAPPINGS)
        .setVersionMetaKey("version")
        .setOrigin("FAKE_ORIGIN")
        .build();

    /**
     * When we upgrade Elasticsearch versions, existing indices may be newly
     * defined as system indices. We need to validate that these indices don't
     * have the "hidden index" setting.
     */
    public void testMakingHiddenIndexIntoSystemIndexFails() {
        // set up a system index upgrade service
        SystemIndexMetadataUpgradeService service = new SystemIndexMetadataUpgradeService(
            new SystemIndices(
                Map.of("MyIndex", new SystemIndices.Feature("foo", "a test feature", List.of(DESCRIPTOR)))),
            mock(ClusterService.class)
        );

        // create an initial cluster state with a hidden index that matches the system index descriptor
        IndexMetadata.Builder hiddenIndexMetadata = IndexMetadata.builder(SYSTEM_INDEX_NAME)
            .settings(getSettingsBuilder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true));

        Metadata.Builder clusterMetadata = new Metadata.Builder();
        clusterMetadata.put(hiddenIndexMetadata);

        ClusterState clusterState = ClusterState.builder(new ClusterName("system-index-metadata-upgrade-service-tests"))
            .metadata(clusterMetadata.build())
            .customs(ImmutableOpenMap.of()).build();

        // Get a metadata upgrade task and execute it on the initial cluster state
        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> service.getTask().execute(clusterState));

        assertThat(exception.getMessage(),
            equalTo("Cannot define index [.myindex-1] as a system index because it has the [index.hidden] setting set to true."));
    }

    private static Settings.Builder getSettingsBuilder() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
    }
}
