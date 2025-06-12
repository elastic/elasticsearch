/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.snapshots;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RestSnapshotsStatusCancellationIT extends AbstractSnapshotRestTestCase {

    public void testSnapshotStatusCancellation() throws Exception {
        internalCluster().startMasterOnlyNode(SINGLE_THREADED_SNAPSHOT_META_SETTINGS);
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createIndex("test-idx");
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "mock");
        final int snapshotCount = randomIntBetween(1, 5);
        final Collection<String> snapshotNames = AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, snapshotCount);

        final MockRepository repository = AbstractSnapshotIntegTestCase.getRepositoryOnMaster(repoName);
        repository.setBlockOnAnyFiles();

        final Request request = new Request(
            HttpGet.METHOD_NAME,
            "/_snapshot/"
                + repoName
                + "/"
                + String.join(",", randomSubsetOf(randomIntBetween(1, snapshotCount), snapshotNames))
                + "/_status"
        );
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        assertFalse(future.isDone());
        awaitTaskWithPrefix(TransportSnapshotsStatusAction.TYPE.name());
        assertBusy(() -> assertTrue(repository.blocked()), 30L, TimeUnit.SECONDS);
        cancellable.cancel();
        assertAllCancellableTasksAreCancelled(TransportSnapshotsStatusAction.TYPE.name());
        repository.unblock();
        expectThrows(CancellationException.class, future::actionGet);

        assertAllTasksHaveFinished(TransportSnapshotsStatusAction.TYPE.name());
    }
}
