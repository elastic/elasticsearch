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
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
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
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end test for the {@code dlm_frozen_transitions} health indicator: verifies that an index eligible for
 * frozen-tier conversion but unable to be marked (because no default snapshot repository is configured) turns the
 * indicator YELLOW with the {@code eligible_indices_unmarked_no_default_repository} diagnosis, and that once the default repository is
 * configured and the transition completes, the indicator returns to GREEN.
 */
@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 0, supportsDedicatedMasters = false, numClientNodes = 0)
public class DLMFrozenTransitionHealthIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(DLMFrozenTransitionHealthIT.class);
    private static final String REPO_NAME = "dlm-frozen-health-e2e-repo";
    private static final String DATA_STREAM_NAME = "dlm-frozen-health-e2e-ds";
    private static final String TEMPLATE_NAME = "dlm-frozen-health-e2e-template";

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

        // Trial license for searchable snapshots
        builder.put("xpack.license.self_generated.type", "trial");

        // Speed up DLM lifecycle polling (marking for frozen)
        builder.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");

        // Speed up frozen transition polling
        builder.put(DLMFrozenTransitionService.POLL_INTERVAL_SETTING.getKey(), "1s");

        // Lower error retry interval
        builder.put(DataStreamLifecycleErrorStore.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.getKey(), "1");

        // Publish frozen-transition health info frequently so conditions surface quickly in the test.
        // We use the default 24-hour stuck threshold and backdate the candidate index's origination date instead.
        builder.put(DLMFrozenTransitionHealthInfoPublisher.PUBLISH_INTERVAL_SETTING.getKey(), "1s");

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

    @After
    public void cleanup() {
        if (cluster().size() == 0) {
            return;
        }
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

    public void testEligibleUnmarkedIndexTurnsIndicatorYellowThenGreenAfterTransition() throws Exception {
        assumeTrue("requires DLM searchable snapshots feature flag", DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled());

        // --- Setup: start nodes (no default repository configured yet) ---
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        startFrozenOnlyNode();

        // --- Create data stream with a tiny frozen_after lifecycle ---
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

        // --- Index a doc, then roll over so generation 1 becomes a non-write index ---
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
        logger.info("--> candidate index for frozen conversion: {}", candidateIndex);

        // Backdate the candidate index so it appears to have been eligible for >24h (the default stuck threshold).
        // This avoids waiting for real wall-clock time to elapse; getGenerationLifecycleDate() prefers origination date.
        updateIndexSettings(
            Settings.builder()
                .put(IndexSettings.LIFECYCLE_ORIGINATION_DATE, System.currentTimeMillis() - TimeValue.timeValueDays(2).millis()),
            candidateIndex
        );

        // --- No default repository is configured: the candidate index is eligible but cannot be marked.
        // The indicator must go YELLOW with the eligible_indices_unmarked_no_default_repository diagnosis. ---
        assertBusy(() -> {
            GetHealthAction.Response healthResponse = client().execute(
                GetHealthAction.INSTANCE,
                new GetHealthAction.Request(DLMFrozenTransitionsHealthIndicatorService.NAME, true, 1000)
            ).actionGet();
            HealthIndicatorResult indicator = healthResponse.findIndicator(DLMFrozenTransitionsHealthIndicatorService.NAME);
            assertThat(indicator.status(), is(HealthStatus.YELLOW));
            Diagnosis diagnosis = indicator.diagnosisList()
                .stream()
                .filter(
                    d -> d.definition()
                        .equals(DLMFrozenTransitionsHealthIndicatorService.ELIGIBLE_INDICES_UNMARKED_NO_REPOSITORY_DIAGNOSIS_DEF)
                )
                .findFirst()
                .orElse(null);
            assertThat("expected an eligible_indices_unmarked_no_default_repository diagnosis", diagnosis, notNullValue());
            assertThat(diagnosis.affectedResources().get(0).getValues(), containsInAnyOrder(candidateIndex));
        }, 60, TimeUnit.SECONDS);

        logger.info("--> confirmed dlm_frozen_transitions indicator is YELLOW due to eligible_indices_unmarked_no_default_repository");

        // --- Configure the default repository so the candidate index can be marked and transitioned ---
        assertAcked(
            client().execute(
                TransportPutRepositoryAction.TYPE,
                new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME).type("fs")
                    .settings(Settings.builder().put("location", randomRepoPath()))
            ).actionGet()
        );
        updateClusterSettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO_NAME));

        // --- Once the transition completes, the indicator must go back to GREEN ---
        assertBusy(() -> {
            GetHealthAction.Response healthResponse = client().execute(
                GetHealthAction.INSTANCE,
                new GetHealthAction.Request(DLMFrozenTransitionsHealthIndicatorService.NAME, true, 1000)
            ).actionGet();
            HealthIndicatorResult indicator = healthResponse.findIndicator(DLMFrozenTransitionsHealthIndicatorService.NAME);
            assertThat(indicator.status(), is(HealthStatus.GREEN));
        }, 60, TimeUnit.SECONDS);

        logger.info("--> confirmed dlm_frozen_transitions indicator returned to GREEN once the transition completed");
    }
}
