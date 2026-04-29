/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.junit.After;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
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
        return Collections.unmodifiableList(plugins);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));

        // Ensure data nodes can serve frozen shards
        if (DiscoveryNode.canContainData(otherSettings)) {
            builder = Settings.builder().put(addRoles(builder.build(), Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)));
            builder.put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.of(10, ByteSizeUnit.MB).getStringRep());
            builder.put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                ByteSizeValue.of(1, ByteSizeUnit.MB).getStringRep()
            );
            builder.put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), false);
        }

        // Trial license for searchable snapshots
        builder.put("xpack.license.self_generated.type", "trial");
        builder.put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB));

        // Speed up DLM lifecycle polling (rollover + marking for frozen)
        builder.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        builder.put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");

        // Speed up frozen transition polling
        builder.put(DLMFrozenTransitionService.POLL_INTERVAL_SETTING.getKey(), "1m");

        // Lower error retry interval
        builder.put(DataStreamLifecycleErrorStore.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.getKey(), "3");

        return builder.build();
    }

    @After
    public void cleanup() {
        // Clear the default repository setting before teardown so that the repository can be deleted
        try {
            updateClusterSettings(Settings.builder().putNull(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey()));
        } catch (Exception e) {
            logger.warn("Failed to clear default repository setting during cleanup", e);
        }
        try {
            client().execute(
                DeleteDataStreamAction.INSTANCE,
                new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { DATA_STREAM_NAME })
            ).actionGet();
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
     *   <li>Index a document to trigger rollover (so generation-1 becomes a non-write index)</li>
     *   <li>Wait for DLM to mark the candidate index with the frozen-candidate repository metadata</li>
     *   <li>Wait for the frozen transition to complete — the frozen snapshot is mounted and in the
     *       data stream, and the original index and clone artefacts are cleaned up</li>
     * </ol>
     */
    public void testEndToEndFrozenTransition() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        // --- Setup: start nodes, register repo, configure default repo ---
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        assertAcked(
            client().execute(
                TransportPutRepositoryAction.TYPE,
                new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).type("fs")
                    .settings(Settings.builder().put("location", randomRepoPath()))
            ).actionGet()
        );
        updateClusterSettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO_NAME));

        // --- Manipulate the DLM clock so indices are immediately past frozen_after ---
        Iterable<DataStreamLifecycleService> dlmServices = internalCluster().getInstances(DataStreamLifecycleService.class);
        Clock clock = Clock.systemUTC();
        AtomicLong now = new AtomicLong(clock.millis());
        dlmServices.forEach(svc -> svc.setNowSupplier(now::get));

        // --- Create data stream with frozen_after lifecycle ---
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .frozenAfter(TimeValue.timeValueSeconds(1))
            .buildTemplate();

        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(TEMPLATE_NAME);
        Settings templateSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
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

        // --- Index a doc to trigger rollover so gen-1 becomes a non-write index ---
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

        // Wait for rollover to produce generation 2
        assertBusy(() -> {
            List<String> indices = getDataStreamBackingIndexNames(DATA_STREAM_NAME);
            assertThat("Expected at least 2 backing indices after rollover", indices.size(), greaterThanOrEqualTo(2));
        }, 30, TimeUnit.SECONDS);

        String candidateIndex = getDataStreamBackingIndexNames(DATA_STREAM_NAME).getFirst();
        logger.info("--> candidate index for frozen conversion: {}", candidateIndex);

        // --- Advance the clock past the frozen_after threshold ---
        now.set(clock.millis() + TimeValue.timeValueDays(1).millis());

        // --- Step 1: Wait for DLM to mark the index for frozen conversion ---
        assertBusy(() -> {
            logger.info("--> checking if index [{}] has been marked for frozen conversion", candidateIndex);
            ClusterStateResponse resp = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
            String repoSet = Optional.ofNullable(resp.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID))
                .map(pm -> pm.index(candidateIndex))
                .map(im -> im.getCustomData(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY))
                .map(meta -> meta.get(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY))
                .orElse(null);
            logger.info("--> frozen candidate repository for [{}]: {}", candidateIndex, repoSet);
            assertThat("Index should be marked with the frozen candidate repository", repoSet, equalTo(REPO_NAME));
        }, 30, TimeUnit.SECONDS);
        logger.info("--> index [{}] successfully marked for frozen conversion", candidateIndex);

        // --- Step 2: Wait for the full conversion to complete ---
        // The frozen index name follows the pattern: dlm-frozen-<original-index-name>
        // The transition service polls every 1 minute, so we allow up to 5 minutes for the full conversion.
        String expectedFrozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + candidateIndex;
        logger.info("--> waiting for frozen index [{}] to appear in data stream", expectedFrozenIndexName);

        assertBusy(() -> {
            ClusterStateResponse resp = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
            var projectMetadata = resp.getState().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
            assertThat("Project metadata should not be null", projectMetadata, notNullValue());

            // Verify the frozen index is now part of the data stream
            GetDataStreamAction.Response dsResp = client().execute(
                GetDataStreamAction.INSTANCE,
                new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { DATA_STREAM_NAME })
            ).actionGet();
            assertThat(dsResp.getDataStreams().size(), equalTo(1));

            List<Index> backingIndices = dsResp.getDataStreams().getFirst().getDataStream().getIndices();
            boolean frozenInDataStream = backingIndices.stream().anyMatch(idx -> idx.getName().equals(expectedFrozenIndexName));
            assertThat("Frozen index should be in the data stream's backing indices", frozenInDataStream, is(true));

            // Verify the original index has been cleaned up (removed from cluster state)
            assertThat(
                "Original index [" + candidateIndex + "] should have been deleted",
                projectMetadata.index(candidateIndex),
                nullValue()
            );

            // Verify the clone index has been cleaned up
            String cloneIndexName = DLMConvertToFrozen.CLONE_INDEX_PREFIX + candidateIndex;
            assertThat("Clone index [" + cloneIndexName + "] should have been deleted", projectMetadata.index(cloneIndexName), nullValue());
            // Verify the original index is no longer in the data stream
            boolean originalInDataStream = backingIndices.stream().anyMatch(idx -> idx.getName().equals(candidateIndex));
            assertThat("Original index should no longer be in the data stream", originalInDataStream, is(false));

            logger.info("--> frozen conversion fully verified for index [{}] -> [{}]", candidateIndex, expectedFrozenIndexName);
            // Verify the frozen index exists and was created by DLM
            IndexMetadata frozenMeta = projectMetadata.index(expectedFrozenIndexName);
            assertThat("Frozen index [" + expectedFrozenIndexName + "] should exist", frozenMeta, notNullValue());
            assertThat(
                "Frozen index should have the DLM-created setting",
                DLMConvertToFrozen.DLM_CREATED_SETTING.get(frozenMeta.getSettings()),
                is(true)
            );

        }, 300, TimeUnit.SECONDS);

        logger.info("--> end-to-end DLM frozen transition test completed successfully");
    }
}
