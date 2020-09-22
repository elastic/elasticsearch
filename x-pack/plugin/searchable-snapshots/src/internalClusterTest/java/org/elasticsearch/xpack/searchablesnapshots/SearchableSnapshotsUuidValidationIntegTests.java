/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SearchableSnapshotsUuidValidationIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    public static class TestPlugin extends Plugin implements ActionPlugin {

        private final RestoreBlockingActionFilter restoreBlockingActionFilter;

        public TestPlugin() {
            restoreBlockingActionFilter = new RestoreBlockingActionFilter();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(restoreBlockingActionFilter);
        }
    }

    public static class RestoreBlockingActionFilter extends org.elasticsearch.action.support.ActionFilter.Simple {
        private final PlainActionFuture<Void> executed = new PlainActionFuture<>();
        private final PlainActionFuture<Void> unblocked = new PlainActionFuture<>();

        @Override
        protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
            if (RestoreSnapshotAction.NAME.equals(action)) {
                executed.onResponse(null);
                unblocked.actionGet();
            }
            return true;
        }

        @Override
        public int order() {
            return 0;
        }

        public void unblock() {
            unblocked.onResponse(null);
        }

        public void awaitExecution() {
            executed.actionGet();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(TestPlugin.class)).collect(Collectors.toList());
    }

    public void testMountFailsIfSnapshotChanged() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepo(fsRepoName);

        final Settings.Builder originalIndexSettings = Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true);
        createAndPopulateIndex(indexName, originalIndexSettings);

        createSnapshot(fsRepoName, snapshotName);

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotName,
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            true
        );

        final ActionFuture<RestoreSnapshotResponse> responseFuture = client().execute(MountSearchableSnapshotAction.INSTANCE, req);

        final RestoreBlockingActionFilter restoreBlockingActionFilter = getBlockingActionFilter();
        restoreBlockingActionFilter.awaitExecution();

        assertAcked(client().admin().cluster().prepareDeleteSnapshot(fsRepoName, snapshotName).get());
        createSnapshot(fsRepoName, snapshotName);

        assertFalse(responseFuture.isDone());
        restoreBlockingActionFilter.unblock();

        assertThat(
            expectThrows(SnapshotRestoreException.class, responseFuture::actionGet).getMessage(),
            containsString("snapshot UUID mismatch")
        );

        assertAcked(client().admin().indices().prepareDelete(indexName));
    }

    private static void createSnapshot(String fsRepoName, String snapshotName) {
        final CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(fsRepoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
    }

    private static RestoreBlockingActionFilter getBlockingActionFilter() {
        for (final ActionFilter filter : internalCluster().getCurrentMasterNodeInstance(ActionFilters.class).filters()) {
            if (filter instanceof RestoreBlockingActionFilter) {
                return (RestoreBlockingActionFilter) filter;
            }
        }
        throw new AssertionError("did not find BlockingActionFilter");
    }

}
