/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.MockLogAppender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;

public class SystemIndicesSnapshotIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SystemIndexTestPlugin.class);
        plugins.add(AnotherSystemIndexTestPlugin.class);
        plugins.add(AssociatedIndicesTestPlugin.class);
        return plugins;
    }

    public void testRestoreSystemIndicesAsGlobalState() {
        createRepository("test-repo", "fs");

        // create index and add a document
        assertAcked(prepareCreate(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        ensureGreen();

        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // run a snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // add another document
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // restore indices as global state without closing the index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify only the original document is restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    public void testSnapshotWithoutGlobalState() {
        createRepository("test-repo", "fs");

        // create index and add a document
        assertAcked(prepareCreate(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));
        assertAcked(prepareCreate("not-a-system-index", 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        ensureGreen();

        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "system index doc");
        indexDoc("not-a-system-index", "1", "purpose", "non system index doc");

        // run a snapshot without global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(false)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        clusterAdmin().prepareGetRepositories("test-repo").get();
        Set<String> snapshottedIndices = clusterAdmin().prepareGetSnapshots("test-repo").get()
            .getSnapshots("test-repo").stream()
            .map(SnapshotInfo::indices)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        assertThat("not-a-system-index", in(snapshottedIndices));
        // TODO: without global state the system index shouldn't be snapshotted (8.0 & later only)
        // assertThat(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, not(in(snapshottedIndices)));
    }

    public void testSnapshotByFeature() {
        createRepository("test-repo", "fs");

        // create indices
        assertAcked(prepareCreate(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));
        assertAcked(prepareCreate(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        ensureGreen();

        // put a document in each one
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snaphost doc");
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot by feature
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setFeatureStates(SystemIndexTestPlugin.class.getSimpleName())
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // add some other documents
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
        assertThat(getDocCount(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // restore indices as global state without closing the index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify only the original document is restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    public void testRestoreByFeature() {
        createRepository("test-repo", "fs");

        // create indices
        assertAcked(prepareCreate(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));
        assertAcked(prepareCreate(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));
        // And one regular index
        final String regularIndex = "test-idx";
        indexDoc(regularIndex, "1", "purpose", "create an index that can be restored");

        ensureGreen();

        // put a document in each one
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snaphost doc");
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // add some other documents
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
        assertThat(getDocCount(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // Delete the regular index so we can restore it
        assertAcked(cluster().client().admin().indices().prepareDelete(regularIndex));

        // restore indices by feature, with only the regular index named explicitly
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices(regularIndex)
            .setFeatureStates("SystemIndexTestPlugin")
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify only the original document is restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));

        // but the non-requested feature should still have its new document
        assertThat(getDocCount(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
    }

    public void testSnapshotAndRestoreAssociatedIndices() {
        createRepository("test-repo", "fs");

        final String regularIndex = "regular-idx";

        // create indices
        assertAcked(prepareCreate(regularIndex, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));
        assertAcked(prepareCreate(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));
        assertAcked(prepareCreate(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        ensureGreen();

        // put a document in each one
        indexDoc(regularIndex, "1", "purpose", "pre-snaphost doc");
        indexDoc(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        indexDoc(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(regularIndex, AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME,
            AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME);

        // snapshot
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setIndices(regularIndex)
            .setFeatureStates(AssociatedIndicesTestPlugin.class.getSimpleName())
            .setWaitForCompletion(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        Set<String> snapshottedIndices = clusterAdmin().prepareGetSnapshots("test-repo").get()
            .getSnapshots("test-repo").stream()
            .map(SnapshotInfo::indices)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
        assertThat(snapshottedIndices, hasItem(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME));
        assertThat(snapshottedIndices, hasItem(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME));

        // add some other documents
        indexDoc(regularIndex, "2", "purpose", "post-snapshot doc");
        indexDoc(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(regularIndex, AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME);

        assertThat(getDocCount(regularIndex), equalTo(2L));
        assertThat(getDocCount(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // And delete the associated index so we can restore it
        assertAcked(client().admin().indices().prepareDelete(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME).get());

        // restore the feature state and its associated index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setIndices(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME)
            .setWaitForCompletion(true)
            .setFeatureStates(AssociatedIndicesTestPlugin.class.getSimpleName())
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify only the original document is restored
        assertThat(getDocCount(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
        assertThat(getDocCount(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME), equalTo(1L));
    }

    public void testRestoreFeatureNotInSnapshot() {
        createRepository("test-repo", "fs");

        // create indices
        assertAcked(prepareCreate(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        ensureGreen();

        // put a document in each one
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snaphost doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        final String fakeFeatureStateName = "NonExistentTestPlugin";
        SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setFeatureStates("SystemIndexTestPlugin", fakeFeatureStateName)
                .get());

        assertThat(exception.getMessage(),
            containsString("requested feature states [[" + fakeFeatureStateName + "]] are not present in snapshot"));
    }

    public void testRestoringSystemIndexByNameIsDeprecated() throws IllegalAccessException {
        createRepository("test-repo", "fs");

        // create system index
        assertAcked(prepareCreate(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, 2, Settings.builder()
            .put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)).put("refresh_interval", 10, TimeUnit.SECONDS)));

        ensureGreen();

        // put a document in system index
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snaphost doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        MockLogAppender mockLogAppender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation.snapshots.RestoreService"), mockLogAppender);
        mockLogAppender.start();
        mockLogAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "restore-system-index-from-snapshot",
            "org.elasticsearch.deprecation.snapshots.RestoreService",
            DeprecationLogger.DEPRECATION,
            "Restoring system indices by name is deprecated. Use feature states instead. System indices: [.test-system-idx]"));

        // restore system index by name, rather than feature state
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices(SystemIndexTestPlugin.SYSTEM_INDEX_NAME)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        mockLogAppender.assertAllExpectationsMatched();
        mockLogAppender.stop();
        Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation.snapshots.RestoreService"), mockLogAppender);

        // verify only the original document is restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    private void assertSnapshotSuccess(CreateSnapshotResponse createSnapshotResponse) {
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
    }

    private long getDocCount(String indexName) {
        return client().admin().indices().prepareStats(indexName).get().getPrimaries().getDocs().getCount();
    }

    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME, "System indices for tests"));
        }

        @Override
        public String getFeatureName() {
            return SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin";
        }
    }

    public static class AnotherSystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".another-test-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME, "System indices for tests"));
        }

        @Override
        public String getFeatureName() {
            return AnotherSystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "Another simple test plugin";
        }
    }

    public static class AssociatedIndicesTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".third-test-system-idx";
        public static final String ASSOCIATED_INDEX_NAME = ".associated-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME, "System & associated indices for tests"));
        }

        @Override
        public Collection<String> getAssociatedIndexPatterns() {
            return Collections.singletonList(ASSOCIATED_INDEX_NAME);
        }

        @Override
        public String getFeatureName() {
            return AssociatedIndicesTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "Another simple test plugin";
        }
    }
}
