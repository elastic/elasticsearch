/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
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
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Disruption integration tests for the DLM frozen transition service.
 * <p>
 * Each test intercepts a specific action using an {@link ActionFilter} to detect which phase the
 * DLM service is currently executing. At that point we simulate a user-level disruption — typically
 * deleting the backing index — and then verify the service handles the disruption gracefully.
 */
@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 0, supportsDedicatedMasters = false, numClientNodes = 0)
public class DLMFrozenTransitionDisruptionIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(DLMFrozenTransitionDisruptionIT.class);
    private static final String REPO_NAME = "dlm-disruption-repo";
    private static final String DATA_STREAM_NAME = "dlm-disruption-ds";
    private static final String TEMPLATE_NAME = "dlm-disruption-template";
    private final CopyOnWriteArrayList<Thread> disruptionThreadsToJoin = new CopyOnWriteArrayList<>();

    /**
     * A test plugin that provides a configurable {@link ActionFilter}. Tests register callbacks
     * via the static {@link #addInterceptor} method; these are invoked when a matching action
     * is executed on any node.
     */
    public static class ActionInterceptorPlugin extends Plugin implements ActionPlugin {

        private static final CopyOnWriteArrayList<ActionInterceptor> INTERCEPTORS = new CopyOnWriteArrayList<>();

        public static void addInterceptor(ActionInterceptor interceptor) {
            INTERCEPTORS.add(interceptor);
        }

        public static void clearInterceptors() {
            INTERCEPTORS.clear();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    for (ActionInterceptor interceptor : INTERCEPTORS) {
                        if (interceptor.actionName().equals(action)) {
                            interceptor.intercept(
                                action,
                                request,
                                listener,
                                (req, lis) -> chain.proceed(task, action, (Request) req, (ActionListener<Response>) lis)
                            );
                            return;
                        }
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    /**
     * Callback interface for action interception.
     */
    public interface ActionInterceptor {
        String actionName();

        @SuppressWarnings("rawtypes")
        void intercept(String action, ActionRequest request, ActionListener listener, BiConsumer<ActionRequest, ActionListener> proceed);
    }

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
        plugins.add(ActionInterceptorPlugin.class);
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

    @Before
    public void resetInterceptors() {
        ActionInterceptorPlugin.clearInterceptors();
    }

    @After
    public void cleanup() {
        ActionInterceptorPlugin.clearInterceptors();
        // Join all disruption threads to ensure they don't outlive this test
        for (Thread t : disruptionThreadsToJoin) {
            try {
                t.join(TimeUnit.SECONDS.toMillis(30));
                if (t.isAlive()) {
                    logger.warn("--> disruption thread [{}] still alive after 30s timeout", t.getName());
                    t.interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        disruptionThreadsToJoin.clear();
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
            // Delete any frozen (searchable snapshot) or clone indices that reference the repository before removing it.
            // We must delete these explicitly because the data stream deletion above may not cover frozen indices
            // that were mounted but not yet swapped into the data stream (e.g. due to disruption).
            var projectMetadata = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .metadata()
                .getProject(Metadata.DEFAULT_PROJECT_ID);
            List<String> indicesToDelete = projectMetadata.indices()
                .keySet()
                .stream()
                .filter(
                    name -> name.startsWith(DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX)
                        || name.startsWith(DLMConvertToFrozen.CLONE_INDEX_PREFIX)
                )
                .toList();
            if (indicesToDelete.isEmpty() == false) {
                client().admin().indices().delete(new DeleteIndexRequest(indicesToDelete.toArray(String[]::new))).actionGet();
            }
        } catch (Exception e) {
            logger.warn("Failed to delete frozen/clone indices during cleanup", e);
        }
        try {
            client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).get();
        } catch (Exception e) {
            logger.warn("Failed to delete repository during cleanup", e);
        }
    }

    /**
     * Detects the "mark read-only" phase by intercepting {@link TransportAddIndexBlockAction}.
     * While that request is in-flight, the test deletes the original backing index.
     * <p>
     * Expected behaviour: the service discovers the index is missing and skips remaining steps
     * without recording a persistent error.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-team/issues/2761")
    public void testDeleteBackingIndexDuringMarkReadOnly() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();
        CountDownLatch latch = registerDeleteIndexInterceptor(TransportAddIndexBlockAction.TYPE.name(), candidateIndex, false);
        triggerRollover();

        assertTrue("AddIndexBlock request was never seen by the interceptor", latch.await(30, TimeUnit.SECONDS));
        assertNoErrorRecorded(candidateIndex);
        logger.info("--> delete-during-mark-read-only disruption handled gracefully");
    }

    /**
     * Detects the "clone" phase by intercepting {@link TransportResizeAction}.
     * Once the resize/clone request is seen, the test deletes the original backing index.
     * <p>
     * Expected behaviour: cloning succeeds but subsequent steps find the original index missing.
     * The {@code IndexNotFoundException} path in {@link DLMConvertToFrozen#run()} skips remaining
     * steps silently, so no persistent error is recorded in the error store.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-team/issues/2761")
    public void testDeleteBackingIndexDuringClone() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();
        CountDownLatch latch = registerDeleteIndexInterceptor(TransportResizeAction.TYPE.name(), candidateIndex, false);
        triggerRollover();

        assertTrue("Resize (clone) request was never seen by the interceptor", latch.await(30, TimeUnit.SECONDS));
        assertNoErrorRecorded(candidateIndex);
        logger.info("--> delete-during-clone disruption handled gracefully");
    }

    /**
     * Detects the "take snapshot" phase by intercepting {@link TransportGetSnapshotsAction}.
     * While that request is in-flight, the test deletes the original backing index.
     * <p>
     * Expected behaviour: subsequent steps discover the original index is missing via an
     * {@code IndexNotFoundException}. The service handles this gracefully without recording
     * a persistent error.
     */
    public void testDeleteBackingIndexDuringSnapshot() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();
        CountDownLatch latch = registerDeleteIndexInterceptor(TransportGetSnapshotsAction.TYPE.name(), candidateIndex, false);
        triggerRollover();

        assertTrue("GetSnapshots request was never seen by the interceptor", latch.await(30, TimeUnit.SECONDS));
        assertNoErrorRecorded(candidateIndex);
        logger.info("--> delete-during-snapshot disruption handled gracefully");
    }

    /**
     * Detects the "force merge" phase by intercepting {@code indices:admin/forcemerge}.
     * While that request is in-flight, the test deletes the original backing index.
     * <p>
     * Expected behaviour: subsequent steps discover the index is missing. The
     * {@code IndexNotFoundException} path in {@link DLMConvertToFrozen#run()} handles this
     * gracefully without recording a persistent error.
     */
    public void testDeleteBackingIndexDuringForceMerge() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();
        // Force merge runs on the data node thread — must delete asynchronously to avoid deadlock
        CountDownLatch latch = registerDeleteIndexInterceptor("indices:admin/forcemerge", candidateIndex, true);
        triggerRollover();

        assertTrue("ForceMerge request was never seen by the interceptor", latch.await(30, TimeUnit.SECONDS));
        assertNoErrorRecorded(candidateIndex);
        logger.info("--> delete-during-force-merge disruption handled gracefully");
    }

    /**
     * Detects the "mount searchable snapshot" phase by intercepting
     * {@code cluster:admin/snapshot/mount}. While that request is in-flight, the test deletes
     * the original backing index.
     * <p>
     * Expected behaviour: the service discovers the index is missing and handles this gracefully
     * without recording a persistent error.
     */
    public void testDeleteBackingIndexDuringMountSnapshot() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();
        CountDownLatch latch = registerDeleteIndexInterceptor("cluster:admin/snapshot/mount", candidateIndex, false);
        triggerRollover();

        assertTrue("MountSearchableSnapshot request was never seen by the interceptor", latch.await(60, TimeUnit.SECONDS));
        assertNoErrorRecorded(candidateIndex);
        logger.info("--> delete-during-mount-snapshot disruption handled gracefully");
    }

    /**
     * Detects the "cleanup" phase by intercepting {@code indices:admin/data_stream/modify}
     * (the action used to swap the old backing index with the mounted frozen index in the
     * data stream). While that request is in-flight, the test deletes the original backing index.
     * <p>
     * Expected behaviour: the original index deletion is already the intent of the cleanup phase.
     * The service handles this gracefully without recording a persistent error.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-team/issues/2761")
    public void testDeleteBackingIndexDuringCleanup() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();
        CountDownLatch latch = registerDeleteIndexInterceptor("indices:admin/data_stream/modify", candidateIndex, false);
        triggerRollover();

        assertTrue("ModifyDataStreams request was never seen by the interceptor", latch.await(60, TimeUnit.SECONDS));
        assertNoErrorRecorded(candidateIndex);
        logger.info("--> delete-during-cleanup disruption handled gracefully");
    }

    /**
     * After the mount phase succeeds but before the data stream modify/swap completes,
     * deletes the newly mounted frozen index. The swap then references a non-existent index.
     */
    public void testDeleteMountedFrozenIndexBeforeSwap() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();
        String expectedFrozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + candidateIndex;

        CountDownLatch latch = registerDisruptionInterceptor(
            "indices:admin/data_stream/modify",
            () -> client().admin().indices().delete(new DeleteIndexRequest(expectedFrozenIndexName)).actionGet(),
            false
        );
        triggerRollover();

        assertTrue("ModifyDataStreams request was never seen by the interceptor", latch.await(60, TimeUnit.SECONDS));
        assertErrorRecorded(candidateIndex);
        logger.info("--> delete-frozen-index-before-swap disruption recorded error as expected");
    }

    /**
     * While the frozen transition is in progress (force-merge phase), triggers a user-initiated
     * rollover on the same data stream.
     * The transition should still complete for the original backing index.
     */
    public void testConcurrentRolloverDuringTransition() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();

        CountDownLatch latch = registerDisruptionInterceptor(
            "indices:admin/forcemerge",
            () -> client().admin().indices().prepareRolloverIndex(DATA_STREAM_NAME).get(),
            true
        );
        triggerRollover();

        assertTrue("ForceMerge request was never seen by the interceptor", latch.await(30, TimeUnit.SECONDS));

        String expectedFrozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + candidateIndex;
        assertBusy(() -> {
            var projectMetadata = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .metadata()
                .getProject(Metadata.DEFAULT_PROJECT_ID);
            assertThat(
                "Frozen index should exist despite concurrent rollover",
                projectMetadata.index(expectedFrozenIndexName),
                notNullValue()
            );
        }, 120, TimeUnit.SECONDS);

        logger.info("--> concurrent rollover during transition handled gracefully");
    }

    /**
     * While the frozen transition is in progress (force-merge phase), updates the lifecycle
     * policy to remove the frozenAfter setting. The service should not crash.
     */
    public void testLifecyclePolicyUpdatedDuringTransition() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        String candidateIndex = setupClusterAndInfrastructure();

        CountDownLatch latch = registerDisruptionInterceptor("indices:admin/forcemerge", () -> {
            // no frozenAfter policy
            DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder().buildTemplate();
            Settings templateSettings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
            TransportPutComposableIndexTemplateAction.Request req = new TransportPutComposableIndexTemplateAction.Request(TEMPLATE_NAME);
            req.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(DATA_STREAM_NAME + "*"))
                    .template(Template.builder().settings(templateSettings).lifecycle(lifecycle))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            );
            client().execute(TransportPutComposableIndexTemplateAction.TYPE, req).actionGet();
        }, true);
        triggerRollover();

        assertTrue("ForceMerge request was never seen by the interceptor", latch.await(30, TimeUnit.SECONDS));
        awaitTransitionCompletion(candidateIndex);

        // The service should not crash — verify it's still running
        DLMFrozenTransitionService transitionService = internalCluster().getCurrentMasterNodeInstance(DLMFrozenTransitionService.class);
        assertTrue("Transition service scheduler should not be cancelled", transitionService.isSchedulerThreadRunning());

        logger.info("--> lifecycle-policy-updated-during-transition disruption handled gracefully");
    }

    // ======= Helpers =======

    /**
     * Starts the required cluster nodes and sets up the data stream infrastructure.
     * Returns the name of the first backing index (candidate for frozen transition after rollover).
     */
    private String setupClusterAndInfrastructure() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        startFrozenOnlyNode();
        int numReplicas = 1; // avoid unassigned shards with only 1 data node
        return setupDataStreamInfrastructure(numReplicas);
    }

    /**
     * Registers an interceptor that deletes the given index when the specified action is seen.
     * If {@code async} is true, the deletion runs on a separate thread (needed when the intercepted
     * action holds a thread that would deadlock on a synchronous delete, e.g. force merge).
     *
     * @return a latch that counts down when the interceptor first fires
     */
    private CountDownLatch registerDeleteIndexInterceptor(String actionName, String indexToDelete, boolean async) {
        return registerDisruptionInterceptor(actionName, () -> {
            try {
                client().admin().indices().delete(new DeleteIndexRequest(indexToDelete)).actionGet();
                logger.info("--> deleted [{}] while [{}] was in flight", indexToDelete, actionName);
            } catch (Exception e) {
                logger.warn("Could not delete index during disruption", e);
            }
        }, async);
    }

    /**
     * Registers an interceptor that runs the given disruption action when the specified action is seen.
     * If {@code async} is true, the disruption action runs on a separate thread.
     *
     * @return a latch that counts down when the interceptor first fires
     */
    private CountDownLatch registerDisruptionInterceptor(String actionName, Runnable disruptionAction, boolean async) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean disruptionDone = new AtomicBoolean(false);

        ActionInterceptorPlugin.addInterceptor(new ActionInterceptor() {
            @Override
            public String actionName() {
                return actionName;
            }

            @Override
            @SuppressWarnings({ "rawtypes" })
            public void intercept(
                String action,
                ActionRequest request,
                ActionListener listener,
                BiConsumer<ActionRequest, ActionListener> proceed
            ) {
                if (latch.getCount() > 0) {
                    logger.info("--> intercepted [{}], running disruption action now", actionName);
                    latch.countDown();
                    if (disruptionDone.compareAndSet(false, true)) {
                        if (async) {
                            Thread t = new Thread(disruptionAction, "test-disruption-" + actionName);
                            disruptionThreadsToJoin.add(t);
                            t.start();
                        } else {
                            disruptionAction.run();
                        }
                    }
                }
                proceed.accept(request, listener);
            }
        });
        return latch;
    }

    /**
     * Waits until the frozen transition for the given index has completed (no longer submitted).
     */
    private void awaitTransitionCompletion(String candidateIndex) throws Exception {
        assertBusy(() -> {
            DLMFrozenTransitionService transitionService = internalCluster().getCurrentMasterNodeInstance(DLMFrozenTransitionService.class);
            assertFalse(
                "Transition should have completed for [" + candidateIndex + "]",
                transitionService.getTransitionExecutor().transitionSubmitted(candidateIndex)
            );
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Asserts that no error has been recorded in the DLM error store for the given index.
     * Awaits transition completion before checking error store.
     */
    private void assertNoErrorRecorded(String candidateIndex) throws Exception {
        awaitTransitionCompletion(candidateIndex);
        DLMFrozenTransitionService transitionService = internalCluster().getCurrentMasterNodeInstance(DLMFrozenTransitionService.class);
        DataStreamLifecycleErrorStore errorStore = transitionService.getErrorStore();
        assertThat(
            "No error should be recorded for a gracefully-skipped index",
            errorStore.getError(Metadata.DEFAULT_PROJECT_ID, candidateIndex),
            nullValue()
        );
    }

    /**
     * Asserts that an error has been recorded in the DLM error store for the given index.
     */
    private void assertErrorRecorded(String candidateIndex) throws Exception {
        assertBusy(() -> {
            DLMFrozenTransitionService transitionService = internalCluster().getCurrentMasterNodeInstance(DLMFrozenTransitionService.class);
            DataStreamLifecycleErrorStore errorStore = transitionService.getErrorStore();
            assertThat(
                "An error should be recorded for the disrupted index",
                errorStore.getError(Metadata.DEFAULT_PROJECT_ID, candidateIndex),
                notNullValue()
            );
        }, 15, TimeUnit.SECONDS);
    }

    /**
     * Sets up the prerequisite infrastructure (repository, index template, data stream) and indexes
     * a document, but does NOT roll over. Returns the name of the first backing index (the one that
     * will become the candidate after rollover).
     * <p>
     * Tests should register their interceptors after calling this method, then call
     * {@link #triggerRollover()} to make the index eligible for frozen transition.
     */
    private String setupDataStreamInfrastructure(int numReplicas) {
        // Create repository and set as default so DLM can use it for snapshots
        assertAcked(
            client().execute(
                TransportPutRepositoryAction.TYPE,
                new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).type("fs")
                    .settings(Settings.builder().put("location", randomRepoPath()))
            ).actionGet()
        );
        updateClusterSettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO_NAME));

        // Create datastream
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .frozenAfter(TimeValue.timeValueSeconds(1))
            .buildTemplate();

        Settings templateSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .build();

        TransportPutComposableIndexTemplateAction.Request putTemplateReq = new TransportPutComposableIndexTemplateAction.Request(
            TEMPLATE_NAME
        );
        putTemplateReq.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(DATA_STREAM_NAME + "*"))
                .template(Template.builder().settings(templateSettings).lifecycle(lifecycle))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateReq).actionGet());

        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, DATA_STREAM_NAME)
            ).actionGet()
        );

        // Index a doc so the backing index has data before we roll over
        String timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(DATA_STREAM_NAME).opType(DocWriteRequest.OpType.CREATE)
                .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, timestamp), XContentType.JSON)
        );
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(1));
        assertThat(bulkResponse.getItems()[0].status(), equalTo(RestStatus.CREATED));
        client().admin().indices().refresh(new RefreshRequest(DATA_STREAM_NAME)).actionGet();

        String candidateIndex = backingIndexNames().getFirst();
        logger.info("--> candidate index (pre-rollover): {}", candidateIndex);
        return candidateIndex;
    }

    /**
     * Rolls over the data stream, making the first backing index a non-write index and therefore
     * eligible for frozen transition once the configured frozenAfter period (1 second) elapses.
     */
    private void triggerRollover() {
        assertAcked(client().admin().indices().prepareRolloverIndex(DATA_STREAM_NAME).get());
        logger.info("--> rollover complete; first backing index is now eligible for frozen transition");
    }

    /**
     * Returns the names of the backing indices for the test data stream.
     */
    private List<String> backingIndexNames() {
        var clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        DataStream ds = clusterState.metadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .dataStreams()
            .get(DLMFrozenTransitionDisruptionIT.DATA_STREAM_NAME);
        if (ds == null) {
            return List.of();
        }
        return ds.getIndices().stream().map(Index::getName).toList();
    }
}
