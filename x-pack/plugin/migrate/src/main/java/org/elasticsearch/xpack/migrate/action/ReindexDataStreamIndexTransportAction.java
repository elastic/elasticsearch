/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate;
import org.elasticsearch.xpack.migrate.MigrateTemplateRegistry;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.READ_ONLY;

public class ReindexDataStreamIndexTransportAction extends HandledTransportAction<
    ReindexDataStreamIndexAction.Request,
    ReindexDataStreamIndexAction.Response> {

    public static final String REINDEX_MAX_REQUESTS_PER_SECOND_KEY = "migrate.data_stream_reindex_max_request_per_second";

    public static final Setting<Float> REINDEX_MAX_REQUESTS_PER_SECOND_SETTING = new Setting<>(
        REINDEX_MAX_REQUESTS_PER_SECOND_KEY,
        Float.toString(1000f),
        s -> {
            if (s.equals("-1")) {
                return Float.POSITIVE_INFINITY;
            } else {
                return Float.parseFloat(s);
            }
        },
        value -> {
            if (value <= 0f) {
                throw new IllegalArgumentException(
                    "Failed to parse value ["
                        + value
                        + "] for setting ["
                        + REINDEX_MAX_REQUESTS_PER_SECOND_KEY
                        + "] "
                        + "must be greater than 0 or -1 for infinite"
                );
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(ReindexDataStreamIndexTransportAction.class);
    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);

    private final ClusterService clusterService;
    private final Client client;
    private final TransportService transportService;
    /*
     * The following is incremented in order to keep track of the current round-robin position for ingest nodes that we send sliced requests
     * to. We bound its random starting value to less than or equal to 2 ^ 30 (the default is Integer.MAX_VALUE or 2 ^ 31 - 1) only so that
     * the unit test doesn't fail if it rolls over Integer.MAX_VALUE (since the node selected is the same for Integer.MAX_VALUE and
     * Integer.MAX_VALUE + 1).
     */
    private final AtomicInteger ingestNodeOffsetGenerator = new AtomicInteger(Randomness.get().nextInt(2 ^ 30));

    @Inject
    public ReindexDataStreamIndexTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            ReindexDataStreamIndexAction.NAME,
            false,
            transportService,
            actionFilters,
            ReindexDataStreamIndexAction.Request::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.clusterService = clusterService;
        this.client = client;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(
        Task task,
        ReindexDataStreamIndexAction.Request request,
        ActionListener<ReindexDataStreamIndexAction.Response> listener
    ) {
        var project = clusterService.state().projectState();
        var sourceIndexName = request.getSourceIndex();
        var destIndexName = generateDestIndexName(sourceIndexName);
        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        IndexMetadata sourceIndex = project.metadata().index(sourceIndexName);
        if (sourceIndex == null) {
            listener.onFailure(new ResourceNotFoundException("source index [{}] does not exist", sourceIndexName));
            return;
        }

        Settings settingsBefore = sourceIndex.getSettings();

        var hasOldVersion = DeprecatedIndexPredicate.getReindexRequiredPredicate(project.metadata(), false, true);
        if (hasOldVersion.test(sourceIndex.getIndex()) == false) {
            logger.warn(
                "Migrating index [{}] with version [{}] is unnecessary as its version is not before [{}]",
                sourceIndexName,
                sourceIndex.getCreationVersion(),
                DeprecatedIndexPredicate.MINIMUM_WRITEABLE_VERSION_AFTER_UPGRADE
            );
        }

        final boolean wasClosed = isClosed(sourceIndex);
        SubscribableListener.<AcknowledgedResponse>newForked(l -> removeMetadataBlocks(sourceIndexName, taskId, l))
            .<OpenIndexResponse>andThen(l -> openIndexIfClosed(sourceIndexName, wasClosed, l, taskId))
            .<AcknowledgedResponse>andThen(l -> setReadOnly(sourceIndexName, l, taskId))
            .<BroadcastResponse>andThen(l -> refresh(sourceIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> deleteDestIfExists(destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> createIndex(sourceIndex, destIndexName, l, taskId))
            .<BulkByScrollResponse>andThen(l -> reindex(sourceIndexName, destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> copyOldSourceSettingsToDest(settingsBefore, destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> copyIndexMetadataToDest(sourceIndexName, destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> sanityCheck(sourceIndexName, destIndexName, l, taskId))
            .<CloseIndexResponse>andThen(l -> closeIndexIfWasClosed(destIndexName, wasClosed, l, taskId))
            .<AcknowledgedResponse>andThen(l -> removeAPIBlocks(sourceIndexName, taskId, l, READ_ONLY))
            .andThenApply(ignored -> new ReindexDataStreamIndexAction.Response(destIndexName))
            .addListener(listener);
    }

    private void openIndexIfClosed(String indexName, boolean isClosed, ActionListener<OpenIndexResponse> listener, TaskId parentTaskId) {
        if (isClosed) {
            logger.debug("Opening index [{}]", indexName);
            var request = new OpenIndexRequest(indexName);
            request.setParentTask(parentTaskId);
            client.execute(OpenIndexAction.INSTANCE, request, listener);
        } else {
            listener.onResponse(null);
        }
    }

    private void closeIndexIfWasClosed(
        String indexName,
        boolean wasClosed,
        ActionListener<CloseIndexResponse> listener,
        TaskId parentTaskId
    ) {
        if (wasClosed) {
            logger.debug("Closing index [{}]", indexName);
            var request = new CloseIndexRequest(indexName);
            request.setParentTask(parentTaskId);
            client.execute(TransportCloseIndexAction.TYPE, request, listener);
        } else {
            listener.onResponse(null);
        }
    }

    private static boolean isClosed(IndexMetadata indexMetadata) {
        return indexMetadata.getState().equals(IndexMetadata.State.CLOSE);
    }

    private void setReadOnly(String sourceIndexName, ActionListener<AcknowledgedResponse> listener, TaskId parentTaskId) {
        logger.debug("Setting read-only on source index [{}]", sourceIndexName);
        addBlockToIndex(READ_ONLY, sourceIndexName, new ActionListener<>() {
            @Override
            public void onResponse(AddIndexBlockResponse response) {
                if (response.isAcknowledged()) {
                    listener.onResponse(null);
                } else {
                    var errorMessage = String.format(Locale.ROOT, "Could not set read-only on source index [%s]", sourceIndexName);
                    listener.onFailure(new ElasticsearchException(errorMessage));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ClusterBlockException || e.getCause() instanceof ClusterBlockException) {
                    // Could fail with a cluster block exception if read-only or read-only-allow-delete is already set
                    // In this case, we can proceed
                    listener.onResponse(null);
                } else {
                    listener.onFailure(e);
                }
            }
        }, parentTaskId);
    }

    private void refresh(String sourceIndexName, ActionListener<BroadcastResponse> listener, TaskId parentTaskId) {
        logger.debug("Refreshing source index [{}]", sourceIndexName);
        var refreshRequest = new RefreshRequest(sourceIndexName);
        refreshRequest.setParentTask(parentTaskId);
        client.execute(RefreshAction.INSTANCE, refreshRequest, listener);
    }

    private void deleteDestIfExists(String destIndexName, ActionListener<AcknowledgedResponse> listener, TaskId parentTaskId) {
        logger.debug("Attempting to delete index [{}]", destIndexName);
        var deleteIndexRequest = new DeleteIndexRequest(destIndexName).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        deleteIndexRequest.setParentTask(parentTaskId);
        var errorMessage = String.format(Locale.ROOT, "Failed to acknowledge delete of index [%s]", destIndexName);
        client.admin().indices().delete(deleteIndexRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    private void createIndex(
        IndexMetadata sourceIndex,
        String destIndexName,
        ActionListener<AcknowledgedResponse> listener,
        TaskId parentTaskId
    ) {
        logger.debug("Creating destination index [{}] for source index [{}]", destIndexName, sourceIndex.getIndex().getName());

        var settingsOverride = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            // remove lifecycle so that ILM does not start processing before the index is added to data stream
            .putNull(IndexMetadata.LIFECYCLE_NAME)
            .build();

        var request = new CreateIndexFromSourceAction.Request(
            sourceIndex.getIndex().getName(),
            destIndexName,
            settingsOverride,
            Map.of(),
            true
        );
        request.setParentTask(parentTaskId);
        var errorMessage = String.format(Locale.ROOT, "Could not create index [%s]", request.destIndex());
        client.execute(CreateIndexFromSourceAction.INSTANCE, request, failIfNotAcknowledged(listener, errorMessage));
    }

    // Visible for testing
    void reindex(String sourceIndexName, String destIndexName, ActionListener<BulkByScrollResponse> listener, TaskId parentTaskId) {
        logger.debug("Reindex to destination index [{}] from source index [{}]", destIndexName, sourceIndexName);
        var reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndexName);
        reindexRequest.setDestPipeline(MigrateTemplateRegistry.REINDEX_DATA_STREAM_PIPELINE_NAME);
        reindexRequest.getSearchRequest().allowPartialSearchResults(false);
        reindexRequest.getSearchRequest().source().fetchSource(true);
        reindexRequest.setDestIndex(destIndexName);
        reindexRequest.setParentTask(parentTaskId);
        reindexRequest.setRequestsPerSecond(clusterService.getClusterSettings().get(REINDEX_MAX_REQUESTS_PER_SECOND_SETTING));
        reindexRequest.setSlices(0); // equivalent to slices=auto in rest api
        // Since we delete the source index on success, we want to fail the whole job if there are _any_ documents that fail to reindex:
        ActionListener<BulkByScrollResponse> checkForFailuresListener = ActionListener.wrap(bulkByScrollResponse -> {
            if (bulkByScrollResponse.getSearchFailures().isEmpty() == false) {
                ScrollableHitSource.SearchFailure firstSearchFailure = bulkByScrollResponse.getSearchFailures().get(0);
                listener.onFailure(
                    new ElasticsearchException(
                        "Failure reading data from {} caused by {}",
                        firstSearchFailure.getReason(),
                        sourceIndexName,
                        firstSearchFailure.getReason().getMessage()
                    )
                );
            } else if (bulkByScrollResponse.getBulkFailures().isEmpty() == false) {
                BulkItemResponse.Failure firstBulkFailure = bulkByScrollResponse.getBulkFailures().get(0);
                listener.onFailure(
                    new ElasticsearchException(
                        "Failure loading data from {} into {} caused by {}",
                        firstBulkFailure.getCause(),
                        sourceIndexName,
                        destIndexName,
                        firstBulkFailure.getCause().getMessage()
                    )
                );
            } else {
                listener.onResponse(bulkByScrollResponse);
            }
        }, listener::onFailure);
        /*
         * Reindex will potentially run a pipeline for each document. If we run all reindex requests on the same node (locally), that
         * becomes a bottleneck. This code round-robins reindex requests to all ingest nodes to spread out the pipeline workload. When a
         * data stream has many indices, this can improve performance a good bit.
         */
        final DiscoveryNode[] ingestNodes = clusterService.state().getNodes().getIngestNodes().values().toArray(DiscoveryNode[]::new);
        if (ingestNodes.length == 0) {
            listener.onFailure(new NoNodeAvailableException("No ingest nodes in cluster"));
        } else {
            DiscoveryNode ingestNode = ingestNodes[Math.floorMod(ingestNodeOffsetGenerator.incrementAndGet(), ingestNodes.length)];
            logger.debug("Sending reindex request to {}", ingestNode.getName());
            transportService.sendRequest(
                ingestNode,
                ReindexAction.NAME,
                reindexRequest,
                new ActionListenerResponseHandler<>(
                    checkForFailuresListener,
                    BulkByScrollResponse::new,
                    TransportResponseHandler.TRANSPORT_WORKER
                )
            );
        }
    }

    private void updateSettings(
        String index,
        Settings.Builder settings,
        ActionListener<AcknowledgedResponse> listener,
        TaskId parentTaskId
    ) {
        var updateSettingsRequest = new UpdateSettingsRequest(settings.build(), index);
        updateSettingsRequest.setParentTask(parentTaskId);
        var errorMessage = String.format(Locale.ROOT, "Could not update settings on index [%s]", index);
        client.admin().indices().updateSettings(updateSettingsRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    private void copyOldSourceSettingsToDest(
        Settings settingsBefore,
        String destIndexName,
        ActionListener<AcknowledgedResponse> listener,
        TaskId parentTaskId
    ) {
        logger.debug("Updating settings on destination index after reindex completes");
        var settings = Settings.builder();
        copySettingOrUnset(settingsBefore, settings, IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
        copySettingOrUnset(settingsBefore, settings, IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey());
        updateSettings(destIndexName, settings, listener, parentTaskId);
    }

    private void copyIndexMetadataToDest(
        String sourceIndexName,
        String destIndexName,
        ActionListener<AcknowledgedResponse> listener,
        TaskId parentTaskId
    ) {
        logger.debug("Copying index metadata to destination index [{}] from source index [{}]", destIndexName, sourceIndexName);
        var request = new CopyLifecycleIndexMetadataAction.Request(TimeValue.MAX_VALUE, sourceIndexName, destIndexName);
        request.setParentTask(parentTaskId);
        var errorMessage = String.format(
            Locale.ROOT,
            "Failed to acknowledge copying index metadata from source [%s] to dest [%s]",
            sourceIndexName,
            destIndexName
        );
        client.execute(CopyLifecycleIndexMetadataAction.INSTANCE, request, failIfNotAcknowledged(listener, errorMessage));
    }

    private static void copySettingOrUnset(Settings settingsBefore, Settings.Builder builder, String setting) {
        // if setting was explicitly added to the source index
        if (settingsBefore.get(setting) != null) {
            // copy it back to the dest index
            builder.copy(setting, settingsBefore);
        } else {
            // otherwise, delete from dest index so that it loads from the settings default
            builder.putNull(setting);
        }
    }

    static String generateDestIndexName(String sourceIndex) {
        String prefix = "migrated-";
        if (sourceIndex.startsWith(".")) {
            return "." + prefix + sourceIndex.substring(1);
        }
        return prefix + sourceIndex;
    }

    private static <U extends AcknowledgedResponse> ActionListener<U> failIfNotAcknowledged(
        ActionListener<U> listener,
        String errorMessage
    ) {
        return listener.delegateFailure((delegate, response) -> {
            if (response.isAcknowledged()) {
                delegate.onResponse(null);
            } else {
                delegate.onFailure(new ElasticsearchException(errorMessage));
            }
        });
    }

    private void addBlockToIndex(
        IndexMetadata.APIBlock block,
        String index,
        ActionListener<AddIndexBlockResponse> listener,
        TaskId parentTaskId
    ) {
        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(block, index);
        addIndexBlockRequest.markVerified(false);
        addIndexBlockRequest.setParentTask(parentTaskId);
        client.admin().indices().execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest, listener);
    }

    /**
     * All metadata blocks need to be removed at the start for the following reasons:
     * 1) If the source index has a metadata only block, the read-only block can't be added.
     * 2) If the source index is read-only and closed, it can't be opened.
     */
    private void removeMetadataBlocks(String indexName, TaskId parentTaskId, ActionListener<AcknowledgedResponse> listener) {
        logger.debug("Removing metadata blocks from index [{}]", indexName);
        removeAPIBlocks(indexName, parentTaskId, listener, METADATA, READ_ONLY);
    }

    private void removeAPIBlocks(
        String indexName,
        TaskId parentTaskId,
        ActionListener<AcknowledgedResponse> listener,
        IndexMetadata.APIBlock... blocks
    ) {
        Settings.Builder settings = Settings.builder();
        Arrays.stream(blocks).forEach(b -> settings.putNull(b.settingName()));
        var updateSettingsRequest = new UpdateSettingsRequest(settings.build(), indexName);
        updateSettingsRequest.setParentTask(parentTaskId);
        client.execute(TransportUpdateSettingsAction.TYPE, updateSettingsRequest, listener);
    }

    private void getIndexDocCount(String index, TaskId parentTaskId, ActionListener<Long> listener) {
        SearchRequest countRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        countRequest.allowPartialSearchResults(false);
        countRequest.source(searchSourceBuilder);
        countRequest.setParentTask(parentTaskId);
        client.search(countRequest, listener.delegateFailure((delegate, response) -> {
            var totalHits = response.getHits().getTotalHits();
            assert totalHits.relation() == TotalHits.Relation.EQUAL_TO;
            delegate.onResponse(totalHits.value());
        }));
    }

    private void sanityCheck(
        String sourceIndexName,
        String destIndexName,
        ActionListener<AcknowledgedResponse> listener,
        TaskId parentTaskId
    ) {
        if (Assertions.ENABLED) {
            logger.debug("Comparing source [{}] and dest [{}] doc counts", sourceIndexName, destIndexName);
            RefreshRequest refreshRequest = new RefreshRequest(destIndexName);
            refreshRequest.setParentTask(parentTaskId);
            client.execute(RefreshAction.INSTANCE, refreshRequest, listener.delegateFailureAndWrap((delegate, ignored) -> {
                getIndexDocCount(sourceIndexName, parentTaskId, delegate.delegateFailureAndWrap((delegate1, sourceCount) -> {
                    getIndexDocCount(destIndexName, parentTaskId, delegate1.delegateFailureAndWrap((delegate2, destCount) -> {
                        assert Objects.equals(sourceCount, destCount)
                            : String.format(
                                Locale.ROOT,
                                "source index [%s] has %d docs and dest [%s] has %d docs",
                                sourceIndexName,
                                sourceCount,
                                destIndexName,
                                destCount
                            );
                        delegate2.onResponse(null);
                    }));
                }));
            }));
        } else {
            listener.onResponse(null);
        }
    }
}
