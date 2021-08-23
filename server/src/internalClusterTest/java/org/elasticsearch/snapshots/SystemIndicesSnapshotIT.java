/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.AssociatedIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.snapshots.SnapshotsService.NO_FEATURE_STATES_VALUE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SystemIndicesSnapshotIT extends AbstractSnapshotIntegTestCase {

    public static final String REPO_NAME = "test-repo";

    private List<String> dataNodes = null;

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
        internalCluster().startMasterOnlyNodes(2);
        dataNodes = internalCluster().startDataOnlyNodes(2);
    }

    /**
     * Test that if a snapshot includes system indices and we restore global state,
     * with no reference to feature state, the system indices are restored too.
     */
    public void testRestoreSystemIndicesAsGlobalState() {
        createRepository(REPO_NAME, "fs");
        // put a document in a system index
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // run a snapshot including global state
        createFullSnapshot(REPO_NAME, "test-snap");

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
        createRepository(REPO_NAME, "fs");
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
        Set<String> snapshottedIndices = clusterAdmin().prepareGetSnapshots(REPO_NAME)
            .get()
            .getSnapshots()
            .stream()
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
        createRepository(REPO_NAME, "fs");
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
     * Take a snapshot with global state but don't restore system indexes. By
     * default, snapshot restorations ignore global state. This means that,
     * for now, the system index is treated as part of the snapshot and must be
     * handled explicitly. Otherwise, as in this test, there will be an
     * exception.
     */
    public void testDefaultRestoreOnlyRegularIndices() {
        createRepository(REPO_NAME, "fs");
        final String regularIndex = "test-idx";

        indexDoc(regularIndex, "1", "purpose", "create an index that can be restored");
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(regularIndex, SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // Delete the regular index so we can restore it
        assertAcked(cluster().client().admin().indices().prepareDelete(regularIndex));

        // restore indices by feature, with only the regular index named explicitly
        SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap").setWaitForCompletion(true).get()
        );

        assertThat(
            exception.getMessage(),
            containsString(
                "cannot restore index [" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "] because an open index with same name already exists"
            )
        );
    }

    /**
     * Take a snapshot with global state but restore features by state.
     */
    public void testRestoreByFeature() {
        createRepository(REPO_NAME, "fs");
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
        createRepository(REPO_NAME, "fs");
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
        Set<String> snapshottedIndices = clusterAdmin().prepareGetSnapshots(REPO_NAME)
            .get()
            .getSnapshots()
            .stream()
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
        createRepository(REPO_NAME, "fs");
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
                .get()
        );

        assertThat(
            exception.getMessage(),
            containsString("requested feature states [[" + fakeFeatureStateName + "]] are not present in snapshot")
        );
    }

    /**
     * Check that directly requesting a system index in a restore request logs a deprecation warning.
     * @throws IllegalAccessException if something goes wrong with the mock log appender
     */
    public void testRestoringSystemIndexByNameIsDeprecated() throws IllegalAccessException {
        createRepository(REPO_NAME, "fs");
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
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "restore-system-index-from-snapshot",
                "org.elasticsearch.deprecation.snapshots.RestoreService",
                Level.WARN,
                "Restoring system indices by name is deprecated. Use feature states instead. System indices: [.test-system-idx]"
            )
        );

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
        createRepository(REPO_NAME, "fs");
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
     * If the list of feature states to restore is left unspecified and we are restoring global state,
     * all feature states should be restored.
     */
    public void testRestoreSystemIndicesAsGlobalStateWithDefaultFeatureStateList() {
        createRepository(REPO_NAME, "fs");
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
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify that the system index is destroyed
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(1L));
    }

    /**
     * If the list of feature states to restore contains only "none" and we are restoring global state,
     * no feature states should be restored.
     *
     * In this test, we explicitly request a regular index to avoid any confusion over the meaning of
     * "all indices."
     */
    public void testRestoreSystemIndicesAsGlobalStateWithEmptyListOfFeatureStates() {
        createRepository(REPO_NAME, "fs");
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
            .setFeatureStates(new String[] { randomFrom("none", "NONE") })
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // verify that the system index still has the updated document, i.e. has not been restored
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
    }

    /**
     * If the list of feature states to restore contains only "none" and we are restoring global state,
     * no feature states should be restored. However, for backwards compatibility, if no index is
     * specified, system indices are included in "all indices." In this edge case, we get an error
     * saying that the system index must be closed, because here it is included in "all indices."
     */
    public void testRestoreSystemIndicesAsGlobalStateWithEmptyListOfFeatureStatesNoIndicesSpecified() {
        createRepository(REPO_NAME, "fs");
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
                .setFeatureStates(new String[] { randomFrom("none", "NONE") })
                .get()
        );

        assertThat(
            exception.getMessage(),
            containsString(
                "cannot restore index ["
                    + SystemIndexTestPlugin.SYSTEM_INDEX_NAME
                    + "] because an open index with same name already exists in the cluster."
            )
        );
    }

    /**
     * When a feature state is restored, all indices that are part of that feature state should be deleted, then the indices in
     * the snapshot should be restored.
     *
     * However, other feature states should be unaffected.
     */
    public void testAllSystemIndicesAreRemovedWhenThatFeatureStateIsRestored() {
        createRepository(REPO_NAME, "fs");
        // Create a system index we'll snapshot and restore
        final String systemIndexInSnapshot = SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "-1";
        indexDoc(systemIndexInSnapshot, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "*");

        // And one we'll snapshot but not restore
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");

        // And a regular index so we can avoid matching all indices on the restore
        final String regularIndex = "regular-index";
        indexDoc(regularIndex, "1", "purpose", "pre-snapshot doc");

        // run a snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // Now index another doc and create another index in the same pattern as the first index
        final String systemIndexNotInSnapshot = SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "-2";
        indexDoc(systemIndexInSnapshot, "2", "purpose", "post-snapshot doc");
        indexDoc(systemIndexNotInSnapshot, "1", "purpose", "post-snapshot doc");

        // Add another doc to the second system index, so we can be sure it hasn't been touched
        indexDoc(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(systemIndexInSnapshot, systemIndexNotInSnapshot, AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // Delete the regular index so we can restore it
        assertAcked(cluster().client().admin().indices().prepareDelete(regularIndex));

        // restore the snapshot
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setFeatureStates("SystemIndexTestPlugin")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // The index we created after the snapshot should be gone
        assertFalse(indexExists(systemIndexNotInSnapshot));
        // And the first index should have a single doc
        assertThat(getDocCount(systemIndexInSnapshot), equalTo(1L));
        // And the system index whose state we didn't restore shouldn't have been touched and still have 2 docs
        assertThat(getDocCount(AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
    }

    public void testSystemIndexAliasesAreAlwaysRestored() {
        createRepository(REPO_NAME, "fs");
        // Create a system index
        final String systemIndexName = SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "-1";
        indexDoc(systemIndexName, "1", "purpose", "pre-snapshot doc");

        // And a regular index
        // And a regular index so we can avoid matching all indices on the restore
        final String regularIndex = "regular-index";
        final String regularAlias = "regular-alias";
        indexDoc(regularIndex, "1", "purpose", "pre-snapshot doc");

        // And make sure they both have aliases
        final String systemIndexAlias = SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "-alias";
        assertAcked(
            client().admin()
                .indices()
                .prepareAliases()
                .addAlias(systemIndexName, systemIndexAlias)
                .addAlias(regularIndex, regularAlias)
                .get()
        );

        // run a snapshot including global state
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // And delete both the indices
        assertAcked(cluster().client().admin().indices().prepareDelete(regularIndex, systemIndexName));

        // Now restore the snapshot with no aliases
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setFeatureStates("SystemIndexTestPlugin")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(false)
            .setIncludeAliases(false)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // The regular index should exist
        assertTrue(indexExists(regularIndex));
        assertFalse(indexExists(regularAlias));
        // And the system index, queried by alias, should have a doc
        assertTrue(indexExists(systemIndexName));
        assertTrue(indexExists(systemIndexAlias));
        assertThat(getDocCount(systemIndexAlias), equalTo(1L));

    }

    /**
     * Tests that the special "none" feature state name cannot be combined with other
     * feature state names, and an error occurs if it's tried.
     */
    public void testNoneFeatureStateMustBeAlone() {
        createRepository(REPO_NAME, "fs");
        // put a document in a system index
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // run a snapshot including global state
        IllegalArgumentException createEx = expectThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
                .setWaitForCompletion(true)
                .setIncludeGlobalState(randomBoolean())
                .setFeatureStates("SystemIndexTestPlugin", "none", "AnotherSystemIndexTestPlugin")
                .get()
        );
        assertThat(
            createEx.getMessage(),
            equalTo(
                "the feature_states value [none] indicates that no feature states should be "
                    + "snapshotted, but other feature states were requested: [SystemIndexTestPlugin, none, AnotherSystemIndexTestPlugin]"
            )
        );

        // create a successful snapshot with global state/all features
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        SnapshotRestoreException restoreEx = expectThrows(
            SnapshotRestoreException.class,
            () -> clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
                .setWaitForCompletion(true)
                .setRestoreGlobalState(randomBoolean())
                .setFeatureStates("SystemIndexTestPlugin", "none")
                .get()
        );
        assertThat(
            restoreEx.getMessage(),
            allOf(
                // the order of the requested feature states is non-deterministic so just check that it includes most of the right stuff
                containsString(
                    "the feature_states value [none] indicates that no feature states should be restored, but other feature states were "
                        + "requested:"
                ),
                containsString("SystemIndexTestPlugin")
            )
        );
    }

    /**
     * Tests that using the special "none" feature state value creates a snapshot with no feature states included
     */
    public void testNoneFeatureStateOnCreation() {
        createRepository(REPO_NAME, "fs");
        final String regularIndex = "test-idx";

        indexDoc(regularIndex, "1", "purpose", "create an index that can be restored");
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(regularIndex, SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .setFeatureStates(randomFrom("none", "NONE"))
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // Verify that the system index was not included
        Set<String> snapshottedIndices = clusterAdmin().prepareGetSnapshots(REPO_NAME)
            .get()
            .getSnapshots()
            .stream()
            .map(SnapshotInfo::indices)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        assertThat(snapshottedIndices, allOf(hasItem(regularIndex), not(hasItem(SystemIndexTestPlugin.SYSTEM_INDEX_NAME))));
    }

    public void testNoneFeatureStateOnRestore() {
        createRepository(REPO_NAME, "fs");
        final String regularIndex = "test-idx";

        indexDoc(regularIndex, "1", "purpose", "create an index that can be restored");
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(regularIndex, SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // Create a snapshot
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // Index another doc into the system index
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "2", "purpose", "post-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
        // And delete the regular index so we can restore it
        assertAcked(cluster().client().admin().indices().prepareDelete(regularIndex));

        // Restore the snapshot specifying the regular index and "none" for feature states
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setIndices(regularIndex)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(randomBoolean())
            .setFeatureStates(randomFrom("none", "NONE"))
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        // The regular index should only have one doc
        assertThat(getDocCount(regularIndex), equalTo(1L));
        // But the system index shouldn't have been touched
        assertThat(getDocCount(SystemIndexTestPlugin.SYSTEM_INDEX_NAME), equalTo(2L));
    }

    /**
     * This test checks a piece of BWC logic, and so should be removed when we block restoring system indices by name.
     *
     * This test checks whether it's possible to change the name of a system index when it's restored by name (rather than by feature state)
     */
    public void testCanRenameSystemIndicesIfRestoredByIndexName() {
        createRepository(REPO_NAME, "fs");
        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "purpose", "pre-snapshot doc");
        refresh(SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // snapshot including our system index
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, "test-snap")
            .setWaitForCompletion(true)
            .setIncludeGlobalState(false)
            .get();
        assertSnapshotSuccess(createSnapshotResponse);

        // Now restore it with a rename
        clusterAdmin().prepareRestoreSnapshot(REPO_NAME, "test-snap")
            .setIndices(SystemIndexTestPlugin.SYSTEM_INDEX_NAME)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(false)
            .setFeatureStates(NO_FEATURE_STATES_VALUE)
            .setRenamePattern(".test-(.+)")
            .setRenameReplacement("restored-$1")
            .get();

        assertTrue("The renamed system index should be present", indexExists("restored-system-idx"));
        assertTrue("The original index should still be present", indexExists(SystemIndexTestPlugin.SYSTEM_INDEX_NAME));
    }

    /**
     * Ensures that if we can only capture a partial snapshot of a system index, then the feature state associated with that index is
     * not included in the snapshot, because it would not be safe to restore that feature state.
     */
    public void testPartialSnapshotsOfSystemIndexRemovesFeatureState() throws Exception {
        final String partialIndexName = SystemIndexTestPlugin.SYSTEM_INDEX_NAME;
        final String fullIndexName = AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME;

        createRepositoryNoVerify(REPO_NAME, "mock");

        // Creating the index that we'll get a partial snapshot of with a bunch of shards
        assertAcked(prepareCreate(partialIndexName, 0, indexSettingsNoReplicas(6)));
        indexDoc(partialIndexName, "1", "purpose", "pre-snapshot doc");
        // And another one with the default
        indexDoc(fullIndexName, "1", "purpose", "pre-snapshot doc");
        ensureGreen();

        // Stop a random data node so we lose a shard from the partial index
        internalCluster().stopRandomDataNode();
        assertBusy(
            () -> assertEquals(ClusterHealthStatus.RED, client().admin().cluster().prepareHealth().get().getStatus()),
            30,
            TimeUnit.SECONDS
        );

        // Get ready to block
        blockMasterFromFinalizingSnapshotOnIndexFile(REPO_NAME);

        // Start a snapshot and wait for it to hit the block, then kill the master to force a failover
        final String partialSnapName = "test-partial-snap";
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, partialSnapName)
            .setIncludeGlobalState(true)
            .setWaitForCompletion(false)
            .setPartial(true)
            .get();
        assertThat(createSnapshotResponse.status(), equalTo(RestStatus.ACCEPTED));
        waitForBlock(internalCluster().getMasterName(), REPO_NAME);
        internalCluster().stopCurrentMasterNode();

        // Now get the snapshot and do our checks
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = client().admin()
                .cluster()
                .prepareGetSnapshots(REPO_NAME)
                .setSnapshots(partialSnapName)
                .get();
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
            assertNotNull(snapshotInfo);
            assertThat(snapshotInfo.failedShards(), lessThan(snapshotInfo.totalShards()));
            List<String> statesInSnapshot = snapshotInfo.featureStates()
                .stream()
                .map(SnapshotFeatureInfo::getPluginName)
                .collect(Collectors.toList());
            assertThat(statesInSnapshot, not(hasItem((new SystemIndexTestPlugin()).getFeatureName())));
            assertThat(statesInSnapshot, hasItem((new AnotherSystemIndexTestPlugin()).getFeatureName()));
        });
    }

    public void testParallelIndexDeleteRemovesFeatureState() throws Exception {
        final String indexToBeDeleted = SystemIndexTestPlugin.SYSTEM_INDEX_NAME;
        final String fullIndexName = AnotherSystemIndexTestPlugin.SYSTEM_INDEX_NAME;
        final String nonsystemIndex = "nonsystem-idx";

        final int nodesInCluster = internalCluster().size();
        // Stop one data node so we only have one data node to start with
        internalCluster().stopNode(dataNodes.get(1));
        dataNodes.remove(1);
        ensureStableCluster(nodesInCluster - 1);

        createRepositoryNoVerify(REPO_NAME, "mock");

        // Creating the index that we'll get a partial snapshot of with a bunch of shards
        assertAcked(prepareCreate(indexToBeDeleted, 0, indexSettingsNoReplicas(6)));
        indexDoc(indexToBeDeleted, "1", "purpose", "pre-snapshot doc");
        // And another one with the default
        indexDoc(fullIndexName, "1", "purpose", "pre-snapshot doc");

        // Now start up a new node and create an index that should get allocated to it
        dataNodes.add(internalCluster().startDataOnlyNode());
        createIndexWithContent(
            nonsystemIndex,
            indexSettingsNoReplicas(2).put("index.routing.allocation.require._name", dataNodes.get(1)).build()
        );
        refresh();
        ensureGreen();

        logger.info("--> Created indices, blocking repo on new data node...");
        blockDataNode(REPO_NAME, dataNodes.get(1));

        // Start a snapshot - need to do this async because some blocks will block this call
        logger.info("--> Blocked repo, starting snapshot...");
        final String partialSnapName = "test-partial-snap";
        ActionFuture<CreateSnapshotResponse> createSnapshotFuture = clusterAdmin().prepareCreateSnapshot(REPO_NAME, partialSnapName)
            .setIndices(nonsystemIndex)
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .setPartial(true)
            .execute();

        logger.info("--> Started snapshot, waiting for block...");
        waitForBlock(dataNodes.get(1), REPO_NAME);

        logger.info("--> Repo hit block, deleting the index...");
        assertAcked(cluster().client().admin().indices().prepareDelete(indexToBeDeleted));

        logger.info("--> Index deleted, unblocking repo...");
        unblockNode(REPO_NAME, dataNodes.get(1));

        logger.info("--> Repo unblocked, checking that snapshot finished...");
        CreateSnapshotResponse createSnapshotResponse = createSnapshotFuture.get();
        logger.info(createSnapshotResponse.toString());
        assertThat(createSnapshotResponse.status(), equalTo(RestStatus.OK));

        logger.info("--> All operations complete, running assertions");
        SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertNotNull(snapshotInfo);
        assertThat(snapshotInfo.indices(), not(hasItem(indexToBeDeleted)));
        List<String> statesInSnapshot = snapshotInfo.featureStates()
            .stream()
            .map(SnapshotFeatureInfo::getPluginName)
            .collect(Collectors.toList());
        assertThat(statesInSnapshot, not(hasItem((new SystemIndexTestPlugin()).getFeatureName())));
        assertThat(statesInSnapshot, hasItem((new AnotherSystemIndexTestPlugin()).getFeatureName()));
    }

    private void assertSnapshotSuccess(CreateSnapshotResponse createSnapshotResponse) {
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
    }

    private long getDocCount(String indexName) {
        return client().admin().indices().prepareStats(indexName).get().getPrimaries().getDocs().getCount();
    }

    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME + "*", "System indices for tests"));
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
        public Collection<AssociatedIndexDescriptor> getAssociatedIndexDescriptors() {
            return Collections.singletonList(new AssociatedIndexDescriptor(ASSOCIATED_INDEX_NAME, "Associated indices"));
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
