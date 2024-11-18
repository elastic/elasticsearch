/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.datastreams.ReindexDataStreamIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

public class TransportReindexDataStreamIndexAction extends HandledTransportAction<
    ReindexDataStreamIndexAction.Request,
    ReindexDataStreamIndexAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportReindexDataStreamIndexAction.class);

    // copied from
    // https://github.com/elastic/kibana/blob/8a8363f02cc990732eb9cbb60cd388643a336bed/x-pack/plugins/upgrade_assistant/server/lib/reindexing/index_settings.ts#L155
    private static final Set<String> DISALLOWED_SETTINGS = Set.of(
        // Private ES settings
        "index.allocation.existing_shards_allocator",
        "index.blocks.write",
        "index.creation_date",
        "index.frozen",
        "index.history.uuid",
        "index.merge.enabled",
        "index.provided_name",
        "index.resize.source.name",
        "index.resize.source.uuid",
        "index.routing.allocation.initial_recovery._id",
        "index.search.throttled",
        "index.source_only",
        "index.shrink.source.name",
        "index.shrink.source.uuid",
        "index.uuid",
        "index.verified_before_close",
        // TODO most snapshot settings will need to be set to a different value in the result index
        "index.store.snapshot.repository_name",
        "index.store.snapshot.snapshot_name",
        "index.store.snapshot.snapshot_uuid",
        "index.store.snapshot.index_name",
        "index.store.snapshot.index_uuid",

        // TODO make sure has new value
        "index.version.created",

        // Deprecated in 9.0
        "index.version.upgraded"
    );

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportReindexDataStreamIndexAction(
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
        IndexMetadata sourceIndex = clusterService.state().getMetadata().index(sourceIndexName);

        if (sourceIndex.getCreationVersion().isLegacyIndexVersion() == false) {
            logger.warn(
                "Reindexing datastream index with non-legacy version, index [{}], version [{}]",
                sourceIndexName,
                sourceIndex.getCreationVersion()
            );
        }

        SubscribableListener.<AcknowledgedResponse>newForked(l -> deleteDestIfExists(destIndexName, l))
            .<CreateIndexResponse>andThen(l -> createIndex(sourceIndex, destIndexName, l))
            .<BulkByScrollResponse>andThen(l -> reindex(sourceIndexName, destIndexName, l))
            .<AcknowledgedResponse>andThen(l -> updateSettings(sourceIndex, destIndexName, l))
            // .<AcknowledgedResponse>andThen(l -> createSnapshot(sourceIndex, sourceIndexName, destIndexName, l))
            .andThenApply(response -> new ReindexDataStreamIndexAction.Response(destIndexName))
            .addListener(listener);
    }

    private void deleteDestIfExists(String destIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.debug("Attempting to delete index [{}]", destIndexName);
        var ignoreUnavailable = IndicesOptions.fromOptions(true, false, false, false);
        var deleteIndexRequest = new DeleteIndexRequest(destIndexName).indicesOptions(ignoreUnavailable)
            .masterNodeTimeout(TimeValue.MAX_VALUE);

        client.admin().indices().delete(deleteIndexRequest, listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.isAcknowledged() == false) {
                delegate.onFailure(
                    new ElasticsearchStatusException("Could not delete index [{}]", RestStatus.INTERNAL_SERVER_ERROR, destIndexName)
                );
            } else {
                delegate.onResponse(null);
            }
        }));
    }

    private void createIndex(IndexMetadata sourceIndex, String destIndexName, ActionListener<CreateIndexResponse> listener) {
        logger.debug("Creating destination index [{}] for source index [{}]", destIndexName, sourceIndex.getIndex().getName());

        var settings = getPreSettings(sourceIndex);
        var createIndexRequest = new CreateIndexRequest(destIndexName, settings);

        // TODO will create fail if exists ?
        client.admin().indices().create(createIndexRequest, listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.isAcknowledged() == false) {
                delegate.onFailure(
                    new ElasticsearchStatusException("Could not create index [{}]", RestStatus.INTERNAL_SERVER_ERROR, destIndexName)
                );
            } else {
                delegate.onResponse(null);
            }
        }));
    }

    private void reindex(String sourceIndexName, String destIndexName, ActionListener<BulkByScrollResponse> listener) {
        logger.debug("Reindex to destination index [{}] from source index [{}]", destIndexName, sourceIndexName);
        var reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndexName);
        reindexRequest.getSearchRequest().allowPartialSearchResults(false);
        reindexRequest.getSearchRequest().source().fetchSource(true);
        reindexRequest.setDestIndex(destIndexName);
        client.execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    private void updateSettings(IndexMetadata sourceIndex, String destIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.debug("Adding settings from source index that could not be added before reindex");

        Settings postSettings = getPostSettings(sourceIndex);
        if (postSettings.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        var updateSettingsRequest = new UpdateSettingsRequest();
        updateSettingsRequest.indices(destIndexName);
        updateSettingsRequest.settings(postSettings);

        client.admin().indices().updateSettings(updateSettingsRequest, listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.isAcknowledged() == false) {
                delegate.onFailure(
                    new ElasticsearchStatusException(
                        "Could not update settings on index [{}]",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        destIndexName
                    )
                );
            }
        }));
    }

    // Filter source index settings to subset of settings that can be included during reindex
    // TODO does tsdb start/end time get set in create request if copied into settings from source index
    private Settings getPreSettings(IndexMetadata sourceIndex) {
        return sourceIndex.getSettings().filter(settingName -> DISALLOWED_SETTINGS.contains(settingName) == false);
    }

    private Settings getPostSettings(IndexMetadata sourceIndex) {
        // TODO
        return Settings.EMPTY;
        // return sourceIndex.getSettings().filter(DISALLOWED_SETTINGS::contains);
    }

    public static String generateDestIndexName(String sourceIndex) {
        return "upgrade-" + sourceIndex;
    }
}
