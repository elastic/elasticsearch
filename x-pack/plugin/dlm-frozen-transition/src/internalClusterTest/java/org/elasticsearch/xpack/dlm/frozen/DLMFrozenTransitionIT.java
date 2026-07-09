/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * End-to-end integration test for the DLM frozen transition framework.
 * <p>
 * This test creates a data stream with a {@code frozen_after} lifecycle configuration, lets the DLM
 * lifecycle service mark the index for frozen conversion, and then lets the {@link DLMFrozenTransitionService}
 * naturally pick up and execute the conversion via its periodic polling. The test validates the outcome
 * by checking cluster state.
 */
@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 0, supportsDedicatedMasters = false, numClientNodes = 0)
public class DLMFrozenTransitionIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(DLMFrozenTransitionIT.class);
    private static final String REPO_NAME = "dlm-frozen-e2e-repo";
    private static final String DATA_STREAM_NAME = "dlm-frozen-e2e-ds";
    public static final String TEMPLATE_NAME = "dlm-frozen-e2e-template";

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(BlobCachePlugin.class);
        plugins.add(LocalStateSearchableSnapshots.class);
        plugins.add(DLMFrozenTransitionDisruptionIT.ActionInterceptorPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));

        // Trial license for searchable snapshots
        builder.put("xpack.license.self_generated.type", "trial");

        // Speed up DLM lifecycle polling (marking for frozen)
        builder.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");

        // Speed up frozen transition polling
        builder.put(DLMFrozenTransitionService.POLL_INTERVAL_SETTING.getKey(), "1s");

        // Lower error retry interval
        builder.put(DataStreamLifecycleErrorStore.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.getKey(), "1");

        return builder.build();
    }

    /**
     * Start a frozen-only node with a small shared blob cache to ensure the frozen transition can
     * run in the test (since it relies on snapshotting).
     */
    private void startFrozenOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_frozen", "ingest"))
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.of(10, ByteSizeUnit.MB).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), false)
            .put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
            .build();
        internalCluster().startNode(nodeSettings);
    }

    @After
    public void cleanup() {
        if (cluster().size() == 0) {
            return;
        }
        // Clear the default repository setting before teardown so that the repository can be deleted
        try {
            updateClusterSettings(Settings.builder().putNull(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey()));
        } catch (Exception e) {
            logger.warn("Failed to clear default repository setting during cleanup", e);
        }
        try {
            client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME))
                .actionGet();
        } catch (Exception e) {
            logger.warn("Failed to delete data stream during cleanup", e);
        }
        try {
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request(TEMPLATE_NAME)
            ).actionGet();
        } catch (Exception e) {
            logger.warn("Failed to delete composable index template during cleanup", e);
        }
        try {
            client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).get();
        } catch (Exception e) {
            logger.warn("Failed to delete repository during cleanup", e);
        }
    }

    /**
     * Full end-to-end test for the DLM frozen transition:
     * <ol>
     *   <li>Register a default snapshot repository</li>
     *   <li>Create a data stream with {@code frozen_after: 1s} so indices are quickly eligible</li>
     *   <li>Index a document and manually roll over (so generation-1 becomes a non-write index)</li>
     *   <li>Wait for the frozen transition to complete — the frozen snapshot is mounted and in the
     *       data stream</li>
     *   <li>Verify that the original index and clone artefacts are cleaned up</li>
     * </ol>
     */
    public void testEndToEndFrozenTransition() throws Exception {
        // --- Setup: start nodes, register repo, configure default repo ---
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        startFrozenOnlyNode();

        assertAcked(
            client().execute(
                TransportPutRepositoryAction.TYPE,
                new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).type("fs")
                    .settings(Settings.builder().put("location", randomRepoPath()))
            ).actionGet()
        );
        updateClusterSettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO_NAME));

        // --- Create data stream with frozen_after lifecycle ---
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .frozenAfter(TimeValue.timeValueSeconds(1))
            .buildTemplate();

        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(TEMPLATE_NAME);
        Settings templateSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(DATA_STREAM_NAME + "*"))
                .template(Template.builder().settings(templateSettings).lifecycle(lifecycle))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());

        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME)
            ).actionGet()
        );

        // --- Index a doc so the backing index has data before we roll over ---
        BulkRequest bulkRequest = new BulkRequest();
        String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        bulkRequest.add(
            new IndexRequest(DLMFrozenTransitionIT.DATA_STREAM_NAME).opType(DocWriteRequest.OpType.CREATE)
                .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
        );
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(1));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + DLMFrozenTransitionIT.DATA_STREAM_NAME;
        BulkItemResponse item = bulkResponse.getItems()[0];
        assertThat(item.getFailureMessage(), nullValue());
        assertThat(item.status(), equalTo(RestStatus.CREATED));
        assertThat(item.getIndex(), startsWith(backingIndexPrefix));
        client().admin().indices().refresh(new RefreshRequest(DATA_STREAM_NAME)).actionGet();

        // Manually roll over the data stream so gen-1 becomes a non-write index
        assertAcked(client().admin().indices().prepareRolloverIndex(DATA_STREAM_NAME).get());

        String candidateIndex = getDataStreamBackingIndexNames(DATA_STREAM_NAME).getFirst();
        logger.info("--> candidate index for frozen conversion: {}", candidateIndex);

        // --- Wait for the frozen transition to complete ---
        // The frozen index name follows the pattern: dlm-frozen-<original-index-name>
        String expectedFrozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + candidateIndex;
        logger.info("--> waiting for frozen index [{}] to appear in data stream", expectedFrozenIndexName);

        String cloneIndexName = DLMConvertToFrozen.CLONE_INDEX_PREFIX + candidateIndex;

        assertBusy(() -> {
            // Fetch fresh cluster state and data stream state on every retry
            ClusterStateResponse stateResp = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
            var projectMetadata = stateResp.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
            assertThat("Project metadata should not be null", projectMetadata, notNullValue());

            GetDataStreamAction.Response dsResp = client().execute(
                GetDataStreamAction.INSTANCE,
                new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { DATA_STREAM_NAME })
            ).actionGet();
            assertThat(dsResp.getDataStreams().size(), equalTo(1));
            List<Index> backingIndices = dsResp.getDataStreams().getFirst().getDataStream().getIndices();

            // Frozen index must be in the data stream
            boolean frozenInDataStream = backingIndices.stream().anyMatch(idx -> idx.getName().equals(expectedFrozenIndexName));
            assertThat("Frozen index should be in the data stream's backing indices", frozenInDataStream, is(true));

            // Original and clone must be fully deleted (all three cleanup steps complete)
            assertThat(
                "Original index [" + candidateIndex + "] should have been deleted",
                projectMetadata.index(candidateIndex),
                nullValue()
            );
            assertThat("Clone index [" + cloneIndexName + "] should have been deleted", projectMetadata.index(cloneIndexName), nullValue());
            boolean originalInDataStream = backingIndices.stream().anyMatch(idx -> idx.getName().equals(candidateIndex));
            assertThat("Original index should no longer be in the data stream", originalInDataStream, is(false));
        }, 60, TimeUnit.SECONDS);

        logger.info("--> frozen index [{}] is now in the data stream and cleanup is complete", expectedFrozenIndexName);

        // Verify the frozen index exists in project metadata and has the DLM-created setting.
        // These properties are stable once the frozen index is in the data stream, so a one-shot read is safe.
        ClusterStateResponse resp = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
        var projectMetadata = resp.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        IndexMetadata frozenMeta = projectMetadata.index(expectedFrozenIndexName);
        assertThat("Frozen index [" + expectedFrozenIndexName + "] should exist", frozenMeta, notNullValue());
        assertThat(
            "Frozen index should have the DLM-created setting",
            DataStreamLifecycleService.DLM_CREATED_SETTING.get(frozenMeta.getSettings()),
            is(true)
        );

        logger.info("--> end-to-end DLM frozen transition test completed successfully");
    }

    /**
     * Verifies that a DLM-created frozen index is not re-marked for frozen transition on a subsequent
     * DLM run. The discriminating assertion is that the frozen index never acquires the
     * {@code dlm_freeze_with} custom metadata (i.e.,
     * {@link DataStreamLifecycleService#indexMarkedForFrozen} stays false), even after the index
     * becomes older than {@code frozen_after}. Without the guard added in
     * {@link DataStreamLifecycleService#candidatesForFrozen}, DLM would mark the frozen index again
     * within one poll cycle of it becoming eligible — so asserting the marker is absent after multiple
     * cycles proves the fix.
     */
    public void testAlreadyTransitionedIndexIsNotReMarkedForFrozen() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        // --- Setup: start nodes, register repo, configure default repo ---
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        startFrozenOnlyNode();

        assertAcked(
            client().execute(
                TransportPutRepositoryAction.TYPE,
                new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).type("fs")
                    .settings(Settings.builder().put("location", randomRepoPath()))
            ).actionGet()
        );
        updateClusterSettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO_NAME));

        // --- Create data stream with frozen_after: 1s so indices are quickly eligible ---
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .frozenAfter(TimeValue.timeValueSeconds(1))
            .buildTemplate();

        TransportPutComposableIndexTemplateAction.Request templateRequest = new TransportPutComposableIndexTemplateAction.Request(
            TEMPLATE_NAME
        );
        Settings templateSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        templateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(DATA_STREAM_NAME + "*"))
                .template(Template.builder().settings(templateSettings).lifecycle(lifecycle))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, templateRequest).actionGet());

        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME)
            ).actionGet()
        );

        // --- Index a doc so the backing index has data, then roll over ---
        BulkRequest bulkRequest = new BulkRequest();
        String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        bulkRequest.add(
            new IndexRequest(DATA_STREAM_NAME).opType(DocWriteRequest.OpType.CREATE)
                .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
        );
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(1));
        client().admin().indices().refresh(new RefreshRequest(DATA_STREAM_NAME)).actionGet();
        assertAcked(client().admin().indices().prepareRolloverIndex(DATA_STREAM_NAME).get());

        String candidateIndex = getDataStreamBackingIndexNames(DATA_STREAM_NAME).getFirst();
        String expectedFrozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + candidateIndex;
        String cloneIndexName = DLMConvertToFrozen.CLONE_INDEX_PREFIX + candidateIndex;
        logger.info("--> waiting for initial frozen transition of [{}] to [{}]", candidateIndex, expectedFrozenIndexName);

        // --- Wait for the initial frozen transition to complete ---
        assertBusy(() -> {
            ClusterStateResponse stateResp = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
            var projectMetadata = stateResp.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
            assertThat(projectMetadata, notNullValue());

            GetDataStreamAction.Response dsResp = client().execute(
                GetDataStreamAction.INSTANCE,
                new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { DATA_STREAM_NAME })
            ).actionGet();
            assertThat(dsResp.getDataStreams().size(), equalTo(1));
            List<Index> backingIndices = dsResp.getDataStreams().getFirst().getDataStream().getIndices();

            assertThat(
                "Frozen index should be in the data stream's backing indices",
                backingIndices.stream().anyMatch(idx -> idx.getName().equals(expectedFrozenIndexName)),
                is(true)
            );
            assertThat("Original index should have been deleted", projectMetadata.index(candidateIndex), nullValue());
            assertThat("Clone index should have been deleted", projectMetadata.index(cloneIndexName), nullValue());
        }, 60, TimeUnit.SECONDS);

        logger.info("--> initial transition complete; frozen index [{}] is in the data stream", expectedFrozenIndexName);

        // Confirm the frozen index carries the DLM_CREATED_SETTING and record its creation date.
        ClusterStateResponse afterTransition = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
        var pmAfterTransition = afterTransition.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        IndexMetadata frozenMeta = pmAfterTransition.index(expectedFrozenIndexName);
        assertThat(frozenMeta, notNullValue());
        assertThat(
            "Frozen index should have the DLM-created setting",
            DataStreamLifecycleService.DLM_CREATED_SETTING.get(frozenMeta.getSettings()),
            is(true)
        );
        long frozenCreationDate = frozenMeta.getCreationDate();

        // Wait until the frozen index is definitively past frozen_after (1s), giving DLM multiple
        // poll cycles (1s each) to run. Without the candidatesForFrozen guard, DLM would mark the
        // frozen index within one of these cycles; with the guard, it is never marked.
        assertBusy(
            () -> assertTrue(
                "Frozen index must be older than frozen_after for the guard to be testable",
                System.currentTimeMillis() - frozenCreationDate > TimeValue.timeValueSeconds(2).millis()
            ),
            30,
            TimeUnit.SECONDS
        );

        // The discriminating assertion: the frozen index must NOT carry the dlm_freeze_with marker.
        // If candidatesForFrozen lacked the DLM_CREATED_SETTING guard, DLM would have set this
        // marker within the window above, and this assertion would fail.
        ClusterStateResponse finalState = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
        var finalPm = finalState.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        IndexMetadata finalFrozenMeta = finalPm.index(expectedFrozenIndexName);
        assertThat(finalFrozenMeta, notNullValue());
        assertFalse(
            "DLM-created frozen index [" + expectedFrozenIndexName + "] must not be re-marked for frozen transition",
            DataStreamLifecycleService.indexMarkedForFrozen(finalFrozenMeta)
        );

        logger.info("--> confirmed frozen index [{}] was not re-marked for transition", expectedFrozenIndexName);
    }

    /**
     * Regression test for {@link DLMFrozenCleanupService}: once an index has been successfully
     * transitioned to a mounted frozen searchable snapshot, the backing snapshot must survive an
     * orphan-cleanup pass — even though the index that was originally snapshotted (the clone /
     * original backing index) no longer exists, having been deleted as part of the very same
     * transition that mounted the snapshot.
     */
    public void testTransitionedIndexSnapshotNotDeletedByCleanup() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        DLMFrozenTransitionDisruptionIT.ActionInterceptorPlugin.clearInterceptors();
        try {
            // --- Setup: start nodes, register repo, configure default repo ---
            internalCluster().startMasterOnlyNode();
            internalCluster().startDataOnlyNodes(2);
            startFrozenOnlyNode();

            assertAcked(
                client().execute(
                    TransportPutRepositoryAction.TYPE,
                    new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).type("fs")
                        .settings(Settings.builder().put("location", randomRepoPath()))
                ).actionGet()
            );
            updateClusterSettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO_NAME));

            // --- Create data stream with frozen_after lifecycle ---
            DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
                .frozenAfter(TimeValue.timeValueSeconds(1))
                .buildTemplate();

            TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
                TEMPLATE_NAME
            );
            Settings templateSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
            request.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(DATA_STREAM_NAME + "*"))
                    .template(Template.builder().settings(templateSettings).lifecycle(lifecycle))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            );
            assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());

            assertAcked(
                client().execute(
                    CreateDataStreamAction.INSTANCE,
                    new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME)
                ).actionGet()
            );

            // --- Index a doc so the backing index has data before we roll over ---
            BulkRequest bulkRequest = new BulkRequest();
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(DATA_STREAM_NAME).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.getItems()[0].getFailureMessage(), nullValue());
            assertThat(bulkResponse.getItems()[0].status(), equalTo(RestStatus.CREATED));
            client().admin().indices().refresh(new RefreshRequest(DATA_STREAM_NAME)).actionGet();

            // Manually roll over the data stream so gen-1 becomes a non-write index
            assertAcked(client().admin().indices().prepareRolloverIndex(DATA_STREAM_NAME).get());

            String candidateIndex = getDataStreamBackingIndexNames(DATA_STREAM_NAME).getFirst();
            String expectedFrozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + candidateIndex;
            String cloneIndexName = DLMConvertToFrozen.CLONE_INDEX_PREFIX + candidateIndex;
            logger.info("--> candidate index for frozen conversion: {}", candidateIndex);

            // --- Wait for the frozen transition to complete ---
            awaitClusterState(state -> {
                var project = state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
                DataStream dataStream = project.dataStreams().get(DATA_STREAM_NAME);
                return dataStream != null
                    && dataStream.getIndices().stream().anyMatch(idx -> idx.getName().equals(expectedFrozenIndexName))
                    && project.index(candidateIndex) == null
                    && project.index(cloneIndexName) == null;
            });

            logger.info("--> frozen transition of [{}] to [{}] complete", candidateIndex, expectedFrozenIndexName);

            // --- Capture the exact snapshot backing the mounted frozen index ---
            ClusterStateResponse afterTransition = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
            var projectMetadata = afterTransition.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
            IndexMetadata frozenMeta = projectMetadata.index(expectedFrozenIndexName);
            assertThat("Frozen index [" + expectedFrozenIndexName + "] should exist", frozenMeta, notNullValue());
            String backingSnapshotUuid = frozenMeta.getSettings()
                .get(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY);
            String backingSnapshotName = frozenMeta.getSettings()
                .get(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY);
            assertThat("Mounted frozen index must record its backing snapshot UUID", backingSnapshotUuid, notNullValue());
            assertThat("Mounted frozen index must record its backing snapshot name", backingSnapshotName, notNullValue());

            assertThat(
                "Backing snapshot should exist in the repository before cleanup runs",
                snapshotUuidsInRepo(),
                hasItem(backingSnapshotUuid)
            );

            // --- Record every DeleteSnapshotRequest issued while cleanup runs ---
            List<String> deletedSnapshotNames = new CopyOnWriteArrayList<>();
            DLMFrozenTransitionDisruptionIT.ActionInterceptorPlugin.addInterceptor(new DLMFrozenTransitionDisruptionIT.ActionInterceptor() {
                @Override
                public String actionName() {
                    return TransportDeleteSnapshotAction.TYPE.name();
                }

                @Override
                @SuppressWarnings("rawtypes")
                public void intercept(
                    String action,
                    ActionRequest request,
                    ActionListener listener,
                    BiConsumer<ActionRequest, ActionListener> proceed
                ) {
                    if (request instanceof DeleteSnapshotRequest deleteSnapshotRequest) {
                        Collections.addAll(deletedSnapshotNames, deleteSnapshotRequest.snapshots());
                    }
                    proceed.accept(request, listener);
                }
            });

            // --- Trigger the orphan-cleanup pass directly; its snapshot/index deletes complete synchronously ---
            DLMFrozenCleanupService cleanupService = internalCluster().getCurrentMasterNodeInstance(DLMFrozenCleanupService.class);
            cleanupService.checkForOrphanedResources();

            // --- Primary assertion: cleanup must never even attempt to delete the backing snapshot ---
            assertThat(
                "Cleanup must not attempt to delete the snapshot backing a live mounted index",
                deletedSnapshotNames,
                not(hasItem(backingSnapshotName))
            );

            // --- End-to-end payload checks: the mount and its data are intact ---
            assertThat(
                "Backing snapshot should still exist in the repository after cleanup",
                snapshotUuidsInRepo(),
                hasItem(backingSnapshotUuid)
            );

            ClusterStateResponse afterCleanup = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
            var projectMetadataAfterCleanup = afterCleanup.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
            assertThat(
                "Mounted frozen index should still exist",
                projectMetadataAfterCleanup.index(expectedFrozenIndexName),
                notNullValue()
            );
            assertThat(
                "Frozen index should still be a backing index of the data stream",
                getDataStreamBackingIndexNames(DATA_STREAM_NAME),
                hasItem(expectedFrozenIndexName)
            );

            assertHitCount(prepareSearch(DATA_STREAM_NAME), 1);

            logger.info("--> confirmed cleanup did not delete the snapshot backing the transitioned index [{}]", expectedFrozenIndexName);
        } finally {
            DLMFrozenTransitionDisruptionIT.ActionInterceptorPlugin.clearInterceptors();
        }
    }

    /**
     * Returns the UUIDs of all {@code dlm-frozen-*} snapshots currently present in {@link #REPO_NAME}.
     */
    private List<String> snapshotUuidsInRepo() {
        return clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, REPO_NAME)
            .setSnapshots(DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + "*")
            .get()
            .getSnapshots()
            .stream()
            .map(snapshotInfo -> snapshotInfo.snapshotId().getUUID())
            .toList();
    }
}
