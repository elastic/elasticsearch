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
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;
import static org.hamcrest.core.IsEqual.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RestGetSnapshotsCancellationIT extends AbstractSnapshotRestTestCase {

    public void testGetSnapshotsCancellation() throws Exception {
        internalCluster().startMasterOnlyNode(SINGLE_THREADED_SNAPSHOT_META_SETTINGS);
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "mock");
        AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(1, 5));

        final MockRepository repository = AbstractSnapshotIntegTestCase.getRepositoryOnMaster(repoName);
        repository.setBlockOnAnyFiles();

        final Request request = new Request(HttpGet.METHOD_NAME, "/_snapshot/" + repoName + "/*");
        if (randomBoolean()) {
            request.addParameter("ignore_unavailable", "true");
        }
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        assertThat(future.isDone(), equalTo(false));
        awaitTaskWithPrefix(TransportGetSnapshotsAction.TYPE.name());
        assertBusy(() -> assertTrue(repository.blocked()), 30L, TimeUnit.SECONDS);
        cancellable.cancel();
        assertAllCancellableTasksAreCancelled(TransportGetSnapshotsAction.TYPE.name());
        repository.unblock();
        expectThrows(CancellationException.class, future::actionGet);

        assertAllTasksHaveFinished(TransportGetSnapshotsAction.TYPE.name());
    }
}
