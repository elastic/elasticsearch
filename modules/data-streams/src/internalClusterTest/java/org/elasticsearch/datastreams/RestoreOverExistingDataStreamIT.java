/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests the guarded atomic delete-and-restore of an existing data stream: the destination data stream and its backing/failure-store
 * indices are removed and the corresponding snapshot data stream is restored under the same name, in one cluster-state update. The
 * restored backing indices keep their names but get entirely new index UUIDs, unlike a regular index's open-to-open history-UUID
 * transition, which keeps the same index UUID.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RestoreOverExistingDataStreamIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-repo";
    private static final String SNAPSHOT_NAME = "test-snap";
    private static final String TEMPLATE_ID = "test-template";
    private static final String DATA_STREAM_NAME = "test-ds";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, DataStreamsPlugin.class);
    }

    public void testRestoreOverExistingDataStreamReplacesBackingIndices() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedDataStream();
        final RestoreTarget restoreTarget = resolveRestoreTarget();
        final Index oldBackingIndex = currentDataStream().getIndices().get(0);

        initializeRestoreOverExistingDataStream(restoreTarget);
        awaitRestoreCompleted();

        final DataStream restored = currentDataStream();
        assertThat(restored.getIndices(), hasSize(1));
        final Index newBackingIndex = restored.getIndices().get(0);
        assertThat("the restored backing index must keep the same name", newBackingIndex.getName(), equalTo(oldBackingIndex.getName()));
        assertThat(
            "the restored backing index must be a new identity, not the deleted one",
            newBackingIndex,
            not(equalTo(oldBackingIndex))
        );

        assertHitCount(prepareSearch(DATA_STREAM_NAME).setSize(0), docCount);

        // the data stream must be genuinely functional afterward, not just green: indexing through it exercises the write alias/routing
        // against the newly-restored backing index rather than any stale reference to the deleted one
        indexDoc();
        refresh(DATA_STREAM_NAME);
        assertHitCount(prepareSearch(DATA_STREAM_NAME).setSize(0), docCount + 1);
    }

    /**
     * Deleting the destination data stream strips it from every existing data-stream alias that referenced it, so the synthetic snapshot
     * metadata built for a guarded restore must carry the snapshot's own data-stream aliases explicitly; there is no other path by which
     * a restored data stream can regain its aliases.
     */
    public void testRestoreOverExistingDataStreamRestoresAliasesFromSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String aliasName = "test-alias";
        createRepositoryAndSnapshottedDataStream(aliasName);
        final RestoreTarget restoreTarget = resolveRestoreTarget();
        assertThat(
            "the alias must actually be present in the snapshot's global metadata for this test to be meaningful",
            restoreTarget.snapshotDataStreamAliases(),
            hasKey(aliasName)
        );

        initializeRestoreOverExistingDataStream(restoreTarget);
        awaitRestoreCompleted();

        final DataStreamAlias restoredAlias = currentDataStreamAliases().get(aliasName);
        assertThat("the restored data stream must regain the alias it had when snapshotted", restoredAlias, notNullValue());
        assertThat(restoredAlias.getDataStreams(), contains(DATA_STREAM_NAME));
    }

    public void testRestoredDataStreamSurvivesNodeRestart() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final int docCount = createRepositoryAndSnapshottedDataStream();
        final RestoreTarget restoreTarget = resolveRestoreTarget();

        initializeRestoreOverExistingDataStream(restoreTarget);
        awaitRestoreCompleted();
        final Index restoredBackingIndex = currentDataStream().getIndices().get(0);

        // forces the new-UUID backing index to be reloaded from its on-disk directory, proving that directory was written completely and
        // correctly rather than left in some stale or partial state by the atomic delete-and-restore
        internalCluster().restartNode(dataNode);
        ensureGreen(DATA_STREAM_NAME);

        assertThat(currentDataStream().getIndices().get(0), equalTo(restoredBackingIndex));
        assertHitCount(prepareSearch(DATA_STREAM_NAME).setSize(0), docCount);
    }

    /**
     * Preserves the existing close/delete safety rule rather than cancelling a conflicting snapshot: rejects before publishing anything
     * if an active snapshot already includes the destination data stream (by name), leaving the destination untouched, and a plain retry
     * after that snapshot finishes must then succeed.
     */
    public void testGuardedDataStreamRestoreRejectsActiveSnapshotConflict() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedDataStream();
        final RestoreTarget restoreTarget = resolveRestoreTarget();

        blockNodeOnAnyFiles(REPOSITORY_NAME, dataNode);
        final ActionFuture<CreateSnapshotResponse> blockingSnapshot = startFullSnapshot(REPOSITORY_NAME, "blocking-snap");
        waitForBlock(dataNode, REPOSITORY_NAME);
        try {
            final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = restoreOverExistingDataStreamFuture(restoreTarget);
            final SnapshotInProgressException e = expectThrows(
                SnapshotInProgressException.class,
                () -> future.actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(e.getMessage(), containsString("being snapshotted"));
        } finally {
            unblockAllDataNodes(REPOSITORY_NAME);
            blockingSnapshot.actionGet(TEST_REQUEST_TIMEOUT);
        }

        assertThat("a rejected guarded restore must leave the destination unchanged", currentDataStream(), notNullValue());
        assertThat(currentDataStream().getIndices(), hasSize(1));

        // the conflict is transient: a plain retry after the snapshot finishes succeeds
        initializeRestoreOverExistingDataStream(restoreTarget);
        awaitRestoreCompleted();
    }

    /**
     * The caller resolves the exact destination {@link DataStream} identity (name plus exact backing/failure index identities) before
     * submitting the guarded restore, precisely so that a destination whose backing indices changed since then (e.g. a rollover) is
     * never silently adopted.
     */
    public void testGuardedDataStreamRestoreRejectsExactIdentityMismatch() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedDataStream();
        final RestoreTarget restoreTarget = resolveRestoreTarget();

        final DataStream staleDestination = restoreTarget.target()
            .destinationDataStream()
            .copy()
            .setBackingIndices(
                restoreTarget.target()
                    .destinationDataStream()
                    .getDataComponent()
                    .copy()
                    .setIndices(List.of(new Index(DATA_STREAM_NAME + "-stale-backing", UUIDs.randomBase64UUID())))
                    .build()
            )
            .build();
        final RestoreTarget staleTarget = restoreTarget.withDestination(staleDestination);

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = restoreOverExistingDataStreamFuture(staleTarget);
        final SnapshotRestoreException e = expectThrows(SnapshotRestoreException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(e.getMessage(), containsString("have changed"));

        assertThat("a rejected guarded restore must leave the destination unchanged", currentDataStream(), notNullValue());

        // a destination that has been deleted entirely (rather than merely changed) gets a distinct, more specific message. The
        // destination is genuinely deleted here, rather than simulated by renaming the resolved destination object, so that this
        // exercises the "no longer exists" branch specifically rather than the guarded restore's same-name precondition
        assertAcked(
            client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME))
                .get()
        );
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> missingFuture = restoreOverExistingDataStreamFuture(
            restoreTarget
        );
        final SnapshotRestoreException missingException = expectThrows(
            SnapshotRestoreException.class,
            () -> missingFuture.actionGet(TEST_REQUEST_TIMEOUT)
        );
        assertThat(missingException.getMessage(), containsString("no longer exists in the cluster state"));
    }

    /**
     * The caller is required to resolve a snapshot data stream and destination of the same name; a mismatched pair (e.g. a caller bug
     * that resolved the wrong snapshot data stream for a destination) must be rejected before anything is deleted, rather than silently
     * deleting the destination and installing an unrelated data stream under its own, different name.
     */
    public void testGuardedDataStreamRestoreRejectsNameMismatch() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedDataStream();
        final RestoreTarget restoreTarget = resolveRestoreTarget();

        final DataStream mismatchedDestination = restoreTarget.target().destinationDataStream().copy().setName("other-ds").build();
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = restoreOverExistingDataStreamFuture(
            restoreTarget.withDestination(mismatchedDestination)
        );
        final SnapshotRestoreException e = expectThrows(SnapshotRestoreException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(e.getMessage(), containsString("guarded restore only supports restoring a data stream over one of the same name"));

        assertThat("a rejected guarded restore must leave the destination unchanged", currentDataStream(), notNullValue());
    }

    /**
     * A retry that supplies the same restore UUID as an already-applied guarded restore observes the correlated
     * {@link RestoreInProgress} entry and must be a no-op rather than a second initialization.
     */
    public void testGuardedDataStreamRestoreIdempotentRetryIsANoOp() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepositoryAndSnapshottedDataStream();
        final RestoreTarget restoreTarget = resolveRestoreTarget();
        final String restoreUUID = UUIDs.randomBase64UUID();

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> first = new PlainActionFuture<>();
        restoreService().restoreOverExistingDataStreams(
            ProjectId.DEFAULT,
            restoreTarget.snapshot(),
            restoreTarget.snapshotInfo(),
            TEST_REQUEST_TIMEOUT,
            restoreUUID,
            List.of(restoreTarget.target()),
            restoreTarget.snapshotDataStreamAliases(),
            first
        );
        first.actionGet(TEST_REQUEST_TIMEOUT);
        final Index firstRestoredIndex = currentDataStream().getIndices().get(0);

        final PlainActionFuture<RestoreService.RestoreCompletionResponse> second = new PlainActionFuture<>();
        restoreService().restoreOverExistingDataStreams(
            ProjectId.DEFAULT,
            restoreTarget.snapshot(),
            restoreTarget.snapshotInfo(),
            TEST_REQUEST_TIMEOUT,
            restoreUUID,
            List.of(restoreTarget.target()),
            restoreTarget.snapshotDataStreamAliases(),
            second
        );
        second.actionGet(TEST_REQUEST_TIMEOUT);

        assertThat(
            "a retry with the same restore UUID must not re-delete-and-restore",
            currentDataStream().getIndices().get(0),
            equalTo(firstRestoredIndex)
        );
    }

    private int createRepositoryAndSnapshottedDataStream() throws Exception {
        return createRepositoryAndSnapshottedDataStream(null);
    }

    private int createRepositoryAndSnapshottedDataStream(@Nullable String aliasName) throws Exception {
        createRepository(REPOSITORY_NAME, "mock");
        // no replicas: this cluster only ever has one data node, and a default-settings template would leave the backing index yellow
        final ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(DATA_STREAM_NAME))
            .template(new Template(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 0).build(), null, null))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request(TEMPLATE_ID).indexTemplate(template)
            ).get()
        );
        final AcknowledgedResponse response = client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME)
        ).get();
        assertAcked(response);

        if (aliasName != null) {
            addAlias(aliasName, DATA_STREAM_NAME);
        }

        final int docCount = randomIntBetween(20, 100);
        for (int i = 0; i < docCount; i++) {
            indexDoc();
        }
        refresh(DATA_STREAM_NAME);
        ensureGreen(DATA_STREAM_NAME);

        createFullSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        return docCount;
    }

    private void indexDoc() {
        prepareIndex(DATA_STREAM_NAME).setOpType(DocWriteRequest.OpType.CREATE)
            .setSource("@timestamp", "2020-01-01T00:00:00Z", "field", "value")
            .get();
    }

    private void addAlias(String aliasName, String dataStreamName) {
        final IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(new AliasActions(AliasActions.Type.ADD).alias(aliasName).index(dataStreamName));
        assertAcked(indicesAdmin().aliases(request).actionGet());
    }

    /**
     * Bundles the identity of the snapshot to restore from with the guarded restore target resolved from it, mirroring the analogous
     * regular-index test's {@code RestoreTarget}. Resolving is separate from publishing the transition so a test can mutate the
     * destination identity (e.g. to simulate a stale caller resolution) before submitting.
     */
    private record RestoreTarget(
        Snapshot snapshot,
        SnapshotInfo snapshotInfo,
        RestoreService.DataStreamRestoreTarget target,
        Map<String, DataStreamAlias> snapshotDataStreamAliases
    ) {
        RestoreTarget withDestination(DataStream destination) {
            return new RestoreTarget(
                snapshot,
                snapshotInfo,
                new RestoreService.DataStreamRestoreTarget(destination, target.snapshotDataStream(), target.indicesToRestore()),
                snapshotDataStreamAliases
            );
        }
    }

    private RestoreTarget resolveRestoreTarget() throws IOException {
        final SnapshotInfo snapshotInfo = getSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME);
        final Snapshot snapshot = new Snapshot(REPOSITORY_NAME, snapshotInfo.snapshotId());
        final RepositoryData repositoryData = getRepositoryData(REPOSITORY_NAME);
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(REPOSITORY_NAME);
        final Metadata snapshotGlobalMetadata = repository.getSnapshotGlobalMetadata(snapshotInfo.snapshotId(), false);
        final DataStream snapshotDataStream = snapshotGlobalMetadata.getProject(ProjectId.DEFAULT).dataStreams().get(DATA_STREAM_NAME);

        final Map<String, RestoreService.DataStreamRestoreTarget.SnapshotIndex> indicesToRestore = new HashMap<>();
        for (Index index : Stream.concat(snapshotDataStream.getIndices().stream(), snapshotDataStream.getFailureIndices().stream())
            .toList()) {
            final IndexId indexId = repositoryData.resolveIndexId(index.getName());
            indicesToRestore.put(
                index.getName(),
                new RestoreService.DataStreamRestoreTarget.SnapshotIndex(
                    indexId,
                    repository.getSnapshotIndexMetaData(repositoryData, snapshotInfo.snapshotId(), indexId)
                )
            );
        }

        final RestoreService.DataStreamRestoreTarget target = new RestoreService.DataStreamRestoreTarget(
            currentDataStream(),
            snapshotDataStream,
            indicesToRestore
        );
        return new RestoreTarget(snapshot, snapshotInfo, target, snapshotGlobalMetadata.getProject(ProjectId.DEFAULT).dataStreamAliases());
    }

    private void initializeRestoreOverExistingDataStream(RestoreTarget restoreTarget) {
        safeGet(restoreOverExistingDataStreamFuture(restoreTarget));
    }

    private PlainActionFuture<RestoreService.RestoreCompletionResponse> restoreOverExistingDataStreamFuture(RestoreTarget restoreTarget) {
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> future = new PlainActionFuture<>();
        restoreService().restoreOverExistingDataStreams(
            ProjectId.DEFAULT,
            restoreTarget.snapshot(),
            restoreTarget.snapshotInfo(),
            TEST_REQUEST_TIMEOUT,
            UUIDs.randomBase64UUID(),
            List.of(restoreTarget.target()),
            restoreTarget.snapshotDataStreamAliases(),
            future
        );
        return future;
    }

    private RestoreService restoreService() {
        return internalCluster().getCurrentMasterNodeInstance(RestoreService.class);
    }

    private DataStream currentDataStream() {
        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        return state.metadata().getProject(ProjectId.DEFAULT).dataStreams().get(DATA_STREAM_NAME);
    }

    private Map<String, DataStreamAlias> currentDataStreamAliases() {
        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        return state.metadata().getProject(ProjectId.DEFAULT).dataStreamAliases();
    }

    private void awaitRestoreCompleted() throws Exception {
        assertBusy(
            () -> assertThat(
                RestoreInProgress.get(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState()).isEmpty(),
                equalTo(true)
            )
        );
        ensureGreen(DATA_STREAM_NAME);
    }
}
