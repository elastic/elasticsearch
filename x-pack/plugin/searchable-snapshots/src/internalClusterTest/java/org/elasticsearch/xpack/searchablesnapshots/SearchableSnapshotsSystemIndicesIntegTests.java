/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchableSnapshotsSystemIndicesIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class);
    }

    public void testCannotMountSystemIndex() throws Exception {
        executeTest(
            TestSystemIndexPlugin.INDEX_NAME,
            SearchableSnapshotsSystemIndicesIntegTests.class.getSimpleName(),
            new OriginSettingClient(client(), ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN)
        );
    }

    public void testCannotMountSnapshotBlobCacheIndex() throws Exception {
        executeTest(SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX, "searchable_snapshots", client());
    }

    private void executeTest(final String indexName, final String featureName, final Client client) throws Exception {
        createAndPopulateIndex(indexName, Settings.builder());

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "fs");

        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numPrimaries = getNumShards(indexName).numPrimaries;
        final SnapshotInfo snapshotInfo = createSnapshot(
            repositoryName,
            snapshotName,
            Collections.singletonList("-*"),
            Collections.singletonList(featureName)
        );
        // NOTE: The below assertion assumes that the only index in the feature is the named one. If that's not the case, this will fail.
        assertThat(snapshotInfo.successfulShards(), equalTo(numPrimaries));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        if (randomBoolean()) {
            assertAcked(client.admin().indices().prepareClose(indexName));
        } else {
            assertAcked(client.admin().indices().prepareDelete(indexName));
        }

        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            TEST_REQUEST_TIMEOUT,
            indexName,
            repositoryName,
            snapshotName,
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean()).build(),
            Strings.EMPTY_ARRAY,
            true,
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );

        final ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> client.execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet()
        );
        assertThat(exception.getMessage(), containsString("system index [" + indexName + "] cannot be mounted as searchable snapshots"));
    }

    public static class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {

        static final String INDEX_NAME = ".test-system-index";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptorUtils.createUnmanaged(INDEX_NAME + "*", "System index for [" + getTestClass().getName() + ']')
            );
        }

        @Override
        public String getFeatureName() {
            return SearchableSnapshotsSystemIndicesIntegTests.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "test plugin";
        }
    }
}
