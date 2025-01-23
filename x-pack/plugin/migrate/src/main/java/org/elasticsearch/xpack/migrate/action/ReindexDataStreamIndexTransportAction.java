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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;

public class ReindexDataStreamIndexTransportAction extends HandledTransportAction<
    ReindexDataStreamIndexAction.Request,
    ReindexDataStreamIndexAction.Response> {

    public static final String REINDEX_MAX_REQUESTS_PER_SECOND_KEY = "migrate.data_stream_reindex_max_request_per_second";

    public static final Setting<Float> REINDEX_MAX_REQUESTS_PER_SECOND_SETTING = new Setting<>(
        REINDEX_MAX_REQUESTS_PER_SECOND_KEY,
        Float.toString(10f),
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
    }

    @Override
    protected void doExecute(
        Task task,
        ReindexDataStreamIndexAction.Request request,
        ActionListener<ReindexDataStreamIndexAction.Response> listener
    ) {
        var sourceIndexName = request.getSourceIndex();
        var destIndexName = generateDestIndexName(sourceIndexName);
        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        IndexMetadata sourceIndex = clusterService.state().getMetadata().index(sourceIndexName);
        Settings settingsBefore = sourceIndex.getSettings();

        var hasOldVersion = DeprecatedIndexPredicate.getReindexRequiredPredicate(clusterService.state().metadata(), false);
        if (hasOldVersion.test(sourceIndex.getIndex()) == false) {
            logger.warn(
                "Migrating index [{}] with version [{}] is unnecessary as its version is not before [{}]",
                sourceIndexName,
                sourceIndex.getCreationVersion(),
                DeprecatedIndexPredicate.MINIMUM_WRITEABLE_VERSION_AFTER_UPGRADE
            );
        }

        if (settingsBefore.getAsBoolean(IndexMetadata.SETTING_BLOCKS_READ, false)) {
            var errorMessage = String.format(Locale.ROOT, "Cannot reindex index [%s] which has a read block.", destIndexName);
            listener.onFailure(new ElasticsearchException(errorMessage));
            return;
        }
        if (settingsBefore.getAsBoolean(IndexMetadata.SETTING_BLOCKS_METADATA, false)) {
            var errorMessage = String.format(Locale.ROOT, "Cannot reindex index [%s] which has a metadata block.", destIndexName);
            listener.onFailure(new ElasticsearchException(errorMessage));
            return;
        }

        SubscribableListener.<AcknowledgedResponse>newForked(l -> setBlockWrites(sourceIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> deleteDestIfExists(destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> createIndex(sourceIndex, destIndexName, l, taskId))
            .<BulkByScrollResponse>andThen(l -> reindex(sourceIndexName, destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> copyOldSourceSettingsToDest(settingsBefore, destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> sanityCheck(sourceIndexName, destIndexName, l, taskId))
            .andThenApply(ignored -> new ReindexDataStreamIndexAction.Response(destIndexName))
            .addListener(listener);
    }

    private void setBlockWrites(String sourceIndexName, ActionListener<AcknowledgedResponse> listener, TaskId parentTaskId) {
        logger.debug("Setting write block on source index [{}]", sourceIndexName);
        addBlockToIndex(WRITE, sourceIndexName, new ActionListener<>() {
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
        reindexRequest.getSearchRequest().allowPartialSearchResults(false);
        reindexRequest.getSearchRequest().source().fetchSource(true);
        reindexRequest.setDestIndex(destIndexName);
        reindexRequest.setParentTask(parentTaskId);
        reindexRequest.setRequestsPerSecond(clusterService.getClusterSettings().get(REINDEX_MAX_REQUESTS_PER_SECOND_SETTING));
        reindexRequest.setSlices(0); // equivalent to slices=auto in rest api
        client.execute(ReindexAction.INSTANCE, reindexRequest, listener);
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
        addIndexBlockRequest.setParentTask(parentTaskId);
        client.admin().indices().execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest, listener);
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
            client.execute(
                RefreshAction.INSTANCE,
                new RefreshRequest(destIndexName),
                listener.delegateFailureAndWrap((delegate, ignored) -> {
                    getIndexDocCount(sourceIndexName, parentTaskId, delegate.delegateFailureAndWrap((delegate1, sourceCount) -> {
                        getIndexDocCount(destIndexName, parentTaskId, delegate1.delegateFailureAndWrap((delegate2, destCount) -> {
                            assert sourceCount == destCount
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
                })
            );
        } else {
            listener.onResponse(null);
        }
    }
}
