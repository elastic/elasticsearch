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
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;

public class SystemIndicesSnapshotIT extends AbstractSnapshotIntegTestCase {

    public static final String REPO_NAME = "test-repo";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SystemIndexTestPlugin.class);
        plugins.add(AnotherSystemIndexTestPlugin.class);
        plugins.add(AssociatedIndicesTestPlugin.class);
        return plugins;
    }

    @Before
    public void setup() {
        createRepository(REPO_NAME, "fs");
    }

    /**
     * Test that if a snapshot includes system indices and we restore global state,
     * with no reference to feature state, the system indices are restored too.
     */
    public void testRestoreSystemIndicesAsGlobalState() {
        // put a document in a system index
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // run a snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // add another document
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // restore snapshot with global state, without closing the system index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify only the original document is restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    /**
     * If we take a snapshot with includeGlobalState set to false, are system indices included?
     */
    public void testSnapshotWithoutGlobalState() {
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "system index doc");
        indexDoc("not-a-system-index", "1", "purpose", "non system index doc");

        // run a snapshot without global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(false)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // check snapshot info for for which
        clusterAdmin().prepareGetRepositories(REPO_NAME).get();
        Set<String> snapshottedIndices = clusterAdmin().prepareGetSnapshots(REPO_NAME).get()
            .getSnapshots(REPO_NAME).stream()
            .map(SnapshotInfo::indices)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        assertThat("not-a-system-index", in(snapshottedIndices));
        // TODO: without global state the system index shouldn't be snapshotted (8.0 & later only)
        // assertThat(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, not(in(snapshottedIndices)));
    }

    /**
     * Test that we can snapshot feature states by name.
     */
    public void testSnapshotByFeature() {
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot by feature
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .setFeatureStates(SystemIndexTestPlugin.class.getSimpleName(), AnotherSystemIndexTestPlugin.class.getSimpleName())
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // add some other documents
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
        assertThat(getDocCount(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // restore indices as global state without closing the index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify only the original document is restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    /**
     * Take a snapshot with global state but restore features by state.
     */
    public void testRestoreByFeature() {
        final String regularIndex = "test-idx";

        indexDoc(regularIndex, "1", "purpose", "create an index that can be restored");
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(regularIndex, SystemIndexTestPlugin.SYSTEM_INDEX_NAME, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
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
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIndices(regularIndex)
            .setFeatureStates("SystemIndexTestPlugin")
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify that the restored system index has only one document
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));

        // but the non-requested feature should still have its new document
        assertThat(getDocCount(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
    }

    /**
     * Test that if a feature state has associated indices, they are included in the snapshot
     * when that feature state is selected.
     */
    public void testSnapshotAndRestoreAssociatedIndices() {
        final String regularIndex = "regular-idx";

        // put documents into a regular index as well as the system index and associated index of a feature
        indexDoc(regularIndex, "1", "purpose", "pre-snapshot doc");
        indexDoc(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        indexDoc(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(regularIndex, AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME, AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME);

        // snapshot
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setFeatureStates(AssociatedIndicesTestPlugin.class.getSimpleName())
            .setWaitForCompletion(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // verify the correctness of the snapshot
        Set<String> snapshottedIndices = clusterAdmin().prepareGetSnapshots(REPO_NAME).get()
            .getSnapshots(REPO_NAME).stream()
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
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setIndices(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME)
            .setWaitForCompletion(true)
            .setFeatureStates(AssociatedIndicesTestPlugin.class.getSimpleName())
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify only the original document is restored
        assertThat(getDocCount(AssociatedIndicesTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
        assertThat(getDocCount(AssociatedIndicesTestPlugin.ASSOCIATED_INDEX_NAME), equalTo(1L));
    }

    /**
     * Check that if we request a feature not in the snapshot, we get an error.
     */
    public void testRestoreFeatureNotInSnapshot() {
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        final String fakeFeatureStateName = "NonExistentTestPlugin";
        SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
                .setWaitForCompletion(true)
                .setFeatureStates("SystemIndexTestPlugin", fakeFeatureStateName)
                .get());

        assertThat(exception.getMessage(),
            containsString("requested feature states [[" + fakeFeatureStateName + "]] are not present in snapshot"));
    }

    /**
     * Check that directly requesting a system index in a restore request logs a deprecation warning.
     * @throws IllegalAccessException if something goes wrong with the mock log appender
     */
    public void testRestoringSystemIndexByNameIsDeprecated() throws IllegalAccessException {
        // put a document in system index
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // Delete the index so we can restore it without requesting the feature state
        assertAcked(client().admin().indices().prepareDelete(SystemIndexTestPlugin.SYSTEM_INDEX_NAME).get());

        // Set up a mock log appender to watch for the log message we expect
        MockLogAppender mockLogAppender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation.snapshots.RestoreService"), mockLogAppender);
        mockLogAppender.start();
        mockLogAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "restore-system-index-from-snapshot",
            "org.elasticsearch.deprecation.snapshots.RestoreService",
            DeprecationLogger.DEPRECATION,
            "Restoring system indices by name is deprecated. Use feature states instead. System indices: [.test-system-idx]"));

        // restore system index by name, rather than feature state
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIndices(SystemIndexTestPlugin.SYSTEM_INDEX_NAME)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // Check that the message was logged and remove log appender
        mockLogAppender.assertAllExpectationsMatched();
        mockLogAppender.stop();
        Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation.snapshots.RestoreService"), mockLogAppender);

        // verify only the original document is restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    /**
     * Check that if a system index matches a rename pattern in a restore request, it's not renamed
     */
    public void testSystemIndicesCannotBeRenamed() {
        final String nonSystemIndex = ".test-non-system-index";
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        indexDoc(nonSystemIndex, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        assertAcked(client().admin().indices().prepareDelete(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, nonSystemIndex).get());

        // Restore using a rename pattern that matches both the regular and the system index
        clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .setRenamePattern(".test-(.+)")
            .setRenameReplacement(".test-restored-$1")
            .get();

        // The original system index and the renamed normal index should exist
        assertTrue("System index not renamed", indexExists(SystemIndexTestPlugin.SYSTEM_INDEX_NAME));
        assertTrue("Non-system index was renamed", indexExists(".test-restored-non-system-index"));

        // The original normal index should still be deleted, and there shouldn't be a renamed version of the system index
        assertFalse("Renamed system index doesn't exist", indexExists(".test-restored-system-index"));
        assertFalse("Original non-system index doesn't exist", indexExists(nonSystemIndex));
    }

    /**
     * If the list of feature states to restore is null and we are restoring global state,
     * all feature states should be restored.
     */
    public void testRestoreSystemIndicesAsGlobalStateWithNullFeatureStateList() {
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // run a snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // add another document
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // restore indices as global state a null list of feature states
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .setFeatureStates((String[]) null)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify that the system index is destroyed
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    /**
     * If the list of feature states to restore is an empty list and we are restoring global state,
     * no feature states should be restored.
     *
     * In this test, we explicitly request a regular index to avoid any confusion over the meaning of
     * "all indices."
     */
    public void testRestoreSystemIndicesAsGlobalStateWithEmptyListOfFeatureStates() {
        String regularIndex = "my-index";
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        indexDoc(regularIndex, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, regularIndex);

        // run a snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // add another document
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        assertAcked(client().admin().indices().prepareDelete(regularIndex).get());
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));

        // restore regular index, with global state and an empty list of feature states
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .setFeatureStates(new String[]{})
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify that the system index still has the updated document, i.e. has not been restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
    }

    /**
     * If the list of feature states to restore is an empty list and we are restoring global state,
     * no feature states should be restored. However, for backwards compatibility, if no index is
     * specified, system indices are included in "all indices." In this edge case, we get an error
     * saying that the system index must be closed, because here it is included in "all indices."
     */
    public void testRestoreSystemIndicesAsGlobalStateWithEmptyListOfFeatureStatesNoIndicesSpecified() {
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // run a snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // restore indices as global state without closing the index
        SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
                .setWaitForCompletion(true)
                .setRestoreGlobalState(true)
                .setFeatureStates(new String[]{})
                .get());

        assertThat(exception.getMessage(), containsString("cannot restore index [" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME
            + "] because an open index with same name already exists in the cluster."));
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
