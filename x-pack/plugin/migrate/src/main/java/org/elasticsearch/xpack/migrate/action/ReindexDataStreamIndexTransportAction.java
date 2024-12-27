/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;

public class ReindexDataStreamIndexTransportAction extends HandledTransportAction<
    ReindexDataStreamIndexAction.Request,
    ReindexDataStreamIndexAction.Response> {

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

        var hasOldVersion = DeprecatedIndexPredicate.getReindexRequiredPredicate(clusterService.state().metadata());
        if (hasOldVersion.test(sourceIndex.getIndex()) == false) {
            logger.warn(
                "Migrating index [{}] with version [{}] is unnecessary as its version is not before [{}]",
                sourceIndexName,
                sourceIndex.getCreationVersion(),
                DeprecatedIndexPredicate.MINIMUM_WRITEABLE_VERSION_AFTER_UPGRADE
            );
        }

        SubscribableListener.<AcknowledgedResponse>newForked(l -> setBlockWrites(sourceIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> deleteDestIfExists(destIndexName, l, taskId))
            .<AcknowledgedResponse>andThen(l -> createIndex(sourceIndex, destIndexName, l, taskId))
            .<BulkByScrollResponse>andThen(l -> reindex(sourceIndexName, destIndexName, l, taskId))
            .<AddIndexBlockResponse>andThen(l -> addBlockIfFromSource(WRITE, settingsBefore, destIndexName, l, taskId))
            .<AddIndexBlockResponse>andThen(l -> addBlockIfFromSource(READ_ONLY, settingsBefore, destIndexName, l, taskId))
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
                    // It's fine if block-writes is already set
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

        // override read-only settings if they exist
        var removeReadOnlyOverride = Settings.builder()
            .putNull(IndexMetadata.SETTING_READ_ONLY)
            .putNull(IndexMetadata.SETTING_BLOCKS_WRITE)
            .build();

        var request = new CreateIndexFromSourceAction.Request(
            sourceIndex.getIndex().getName(),
            destIndexName,
            removeReadOnlyOverride,
            Map.of()
        );
        request.setParentTask(parentTaskId);
        var errorMessage = String.format(Locale.ROOT, "Could not create index [%s]", request.getDestIndex());
        client.execute(CreateIndexFromSourceAction.INSTANCE, request, failIfNotAcknowledged(listener, errorMessage));
    }

    private void reindex(String sourceIndexName, String destIndexName, ActionListener<BulkByScrollResponse> listener, TaskId parentTaskId) {
        logger.debug("Reindex to destination index [{}] from source index [{}]", destIndexName, sourceIndexName);
        var reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndexName);
        reindexRequest.getSearchRequest().allowPartialSearchResults(false);
        reindexRequest.getSearchRequest().source().fetchSource(true);
        reindexRequest.setDestIndex(destIndexName);
        reindexRequest.setParentTask(parentTaskId);
        client.execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    private void addBlockIfFromSource(
        IndexMetadata.APIBlock block,
        Settings settingsBefore,
        String destIndexName,
        ActionListener<AddIndexBlockResponse> listener,
        TaskId parentTaskId
    ) {
        if (settingsBefore.getAsBoolean(block.settingName(), false)) {
            var errorMessage = String.format(Locale.ROOT, "Add [%s] block to index [%s] was not acknowledged", block.name(), destIndexName);
            addBlockToIndex(block, destIndexName, failIfNotAcknowledged(listener, errorMessage), parentTaskId);
        } else {
            listener.onResponse(null);
        }
    }

    public static String generateDestIndexName(String sourceIndex) {
        return "migrated-" + sourceIndex;
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
}
