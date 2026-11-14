/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.ShardRestoringException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.unblockAllDataNodes;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The set-up shared by the tests of what happens to an API when its index is being restored from a snapshot on a stateless cluster. A
 * restore that asks for its shards to be reported makes the API fail with a 403; one that does not leaves it failing as it always did.
 *
 * <p>In stateless a restoring primary is on an index node, which is not searchable, and its search copy is only assigned once the
 * primary is active. There is therefore no copy of the restoring shard to send a read to, so the coordinator fails the read without
 * asking any node. A stateful cluster sends the read to the initializing primary instead.
 */
public abstract class AbstractStatelessRestoringShardIT extends AbstractStatelessPluginIntegTestCase {

    protected static final String REPO = "test-repo";
    protected static final String SNAPSHOT = "test-snapshot";
    protected static final String RESTORED_INDEX = "restored-index";
    /**
     * An index that stays healthy while {@link #RESTORED_INDEX} is restored, for the APIs that read several indices at once.
     */
    protected static final String HEALTHY_INDEX = "healthy-index";

    /**
     * The REST tests need a real HTTP transport.
     */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    /**
     * The base class registers its own repository type named {@code mock}, an object store backed by the file system, which cannot block.
     * The snapshot repository here has to, to hold the restore in flight, so it takes that name instead.
     */
    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    /**
     * Creates a single-shard index with documents, snapshots it and deletes it.
     */
    protected void setUpSnapshot() {
        setUpSnapshot(1, false);
    }

    /**
     * Like {@link #setUpSnapshot()}, with {@link #RESTORED_INDEX} split into {@code shards} shards, and with {@link #HEALTHY_INDEX} as
     * well if {@code withHealthyIndex} is set.
     *
     * <p>Every shard gets documents: an empty shard reads nothing from the repository, so its restore would not wait on the block and
     * would finish straight away.
     */
    protected void setUpSnapshot(int shards, boolean withHealthyIndex) {
        startMasterOnlyNode();
        startIndexNodes(1);
        startSearchNode();
        createIndex(RESTORED_INDEX, indexSettings(shards, 1).build());
        ensureGreen(RESTORED_INDEX);
        indexDocs(RESTORED_INDEX, 100 * shards);
        flush(RESTORED_INDEX);
        for (ShardStats shardStats : indicesAdmin().prepareStats(RESTORED_INDEX).clear().setDocs(true).get().getShards()) {
            if (shardStats.getShardRouting().primary()) {
                assertThat(shardStats.getShardRouting().toString(), shardStats.getStats().getDocs().getCount(), greaterThan(0L));
            }
        }
        if (withHealthyIndex) {
            createIndex(HEALTHY_INDEX, indexSettings(1, 1).build());
            ensureGreen(HEALTHY_INDEX);
            indexDocs(HEALTHY_INDEX, 100);
            refresh(HEALTHY_INDEX);
        }
        createRepository(REPO, "mock");
        createSnapshot(REPO, SNAPSHOT, List.of(RESTORED_INDEX), List.of());
        assertAcked(indicesAdmin().prepareDelete(RESTORED_INDEX));
    }

    /**
     * Blocks the repository, starts a restore of {@link #RESTORED_INDEX} and waits until every one of its primaries is initializing from
     * the snapshot.
     *
     * @return the restore UUID, which the exception carries as its recovery ID
     */
    protected String blockAndStartRestore(boolean reportShardRestoring) {
        final String restoreUuid = AbstractSnapshotIntegTestCase.blockAndStartRestore(REPO, SNAPSHOT, RESTORED_INDEX, reportShardRestoring);
        awaitClusterState(state -> {
            final IndexRoutingTable indexRouting = state.routingTable().index(RESTORED_INDEX);
            for (int shard = 0; shard < indexRouting.size(); shard++) {
                final ShardRouting primary = indexRouting.shard(shard).primaryShard();
                if (primary.state() != ShardRoutingState.INITIALIZING
                    || primary.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                    return false;
                }
            }
            return true;
        });
        return restoreUuid;
    }

    protected void finishRestore() throws Exception {
        unblockAllDataNodes(REPO);
        ensureGreen(RESTORED_INDEX);
    }

    /**
     * Runs {@code body} while a single-shard {@link #RESTORED_INDEX} is being restored by a restore that does or does not report its
     * shards, passing it the restore UUID.
     */
    protected void whileRestoring(boolean reportShardRestoring, CheckedConsumer<String, Exception> body) throws Exception {
        whileRestoring(false, reportShardRestoring, body);
    }

    /**
     * Like {@link #whileRestoring(boolean, CheckedConsumer)}, with {@link #HEALTHY_INDEX} as well if {@code withHealthyIndex} is set.
     */
    protected void whileRestoring(boolean withHealthyIndex, boolean reportShardRestoring, CheckedConsumer<String, Exception> body)
        throws Exception {
        setUpSnapshot(1, withHealthyIndex);
        final String restoreUuid = blockAndStartRestore(reportShardRestoring);
        try {
            body.accept(restoreUuid);
        } finally {
            finishRestore();
        }
    }

    protected static void assertShardRestoringException(Throwable e, String restoreUuid) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.FORBIDDEN));
        final var restoring = ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException);
        assertTrue("no ShardRestoringException in the causes of " + e, restoring.isPresent());
        assertThat(((ShardRestoringException) restoring.get()).recoveryId(), equalTo(restoreUuid));
    }

    /**
     * Asserts the failure a restoring shard has always produced: a 503 with no {@link ShardRestoringException} among its causes.
     */
    protected static void assertServiceUnavailableException(Throwable e) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertFalse(
            "unexpected ShardRestoringException in the causes of " + e,
            ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException).isPresent()
        );
    }

    /**
     * Performs a request that is expected to fail with {@code status} and returns its error body, or {@code null} for a HEAD, which has
     * none.
     */
    protected static ObjectPath performExpectingError(Request request, int status) throws IOException {
        final ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(request.getMethod() + " " + request.getEndpoint(), e.getResponse().getStatusLine().getStatusCode(), equalTo(status));
        if (request.getMethod().equals("HEAD")) {
            return null;
        }
        return ObjectPath.createFromResponse(e.getResponse());
    }

    /**
     * Performs a request that is expected to succeed with a 200, such as one that reports failures inside its body, and returns the body.
     */
    protected static ObjectPath performExpectingOk(Request request) throws IOException {
        final Response response = getRestClient().performRequest(request);
        assertThat(request.getMethod() + " " + request.getEndpoint(), response.getStatusLine().getStatusCode(), equalTo(200));
        return ObjectPath.createFromResponse(response);
    }
}
