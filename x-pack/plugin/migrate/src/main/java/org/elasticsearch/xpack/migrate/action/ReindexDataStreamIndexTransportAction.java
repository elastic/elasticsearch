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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ReindexDataStreamIndexTransportAction extends HandledTransportAction<
    ReindexDataStreamIndexAction.Request,
    ReindexDataStreamIndexAction.Response> {

    private static final Logger logger = LogManager.getLogger(ReindexDataStreamIndexTransportAction.class);

    private static final Set<String> SETTINGS_TO_ADD_BACK = Set.of(IndexMetadata.SETTING_BLOCKS_WRITE, IndexMetadata.SETTING_READ_ONLY);

    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);
    private final ClusterService clusterService;
    private final Client client;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public ReindexDataStreamIndexTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client,
        IndexScopedSettings indexScopedSettings
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
        this.indexScopedSettings = indexScopedSettings;
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
        Settings settingsBefore = sourceIndex.getSettings();

        if (sourceIndex.getCreationVersion().isLegacyIndexVersion() == false) {
            logger.warn(
                "Reindexing datastream index with non-legacy version, index [{}], version [{}]",
                sourceIndexName,
                sourceIndex.getCreationVersion()
            );
        }

        SubscribableListener.<AcknowledgedResponse>newForked(l -> setReadOnly(sourceIndexName, l))
            .<AcknowledgedResponse>andThen(l -> deleteDestIfExists(destIndexName, l))
            .<CreateIndexResponse>andThen(l -> createIndex(sourceIndex, destIndexName, l))
            .<BulkByScrollResponse>andThen(l -> reindex(sourceIndexName, destIndexName, l))
            .<AcknowledgedResponse>andThen(l -> updateSettings(settingsBefore, destIndexName, l))
            .andThenApply(ignored -> new ReindexDataStreamIndexAction.Response(destIndexName))
            .addListener(listener);
    }

    private void setReadOnly(String sourceIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.info("Setting read only on source index [{}]", sourceIndexName);
        final Settings readOnlySettings = Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build();
        var updateSettingsRequest = new UpdateSettingsRequest(readOnlySettings, sourceIndexName);
        var errorMessage = String.format(Locale.ROOT, "Could not set read-only on source index [%s]", sourceIndexName);
        client.admin().indices().updateSettings(updateSettingsRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                if (response.isAcknowledged()) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new ElasticsearchException(errorMessage));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ClusterBlockException || e.getCause() instanceof ClusterBlockException) {
                    // It's fine if read-only is already set
                    listener.onResponse(null);
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    private void deleteDestIfExists(String destIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.info("Attempting to delete index [{}]", destIndexName);
        var deleteIndexRequest = new DeleteIndexRequest(destIndexName).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        var errorMessage = String.format(Locale.ROOT, "Failed to acknowledge delete of index [%s]", destIndexName);
        client.admin().indices().delete(deleteIndexRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    private void createIndex(IndexMetadata sourceIndex, String destIndexName, ActionListener<CreateIndexResponse> listener) {
        logger.info("Creating destination index [{}] for source index [{}]", destIndexName, sourceIndex.getIndex().getName());

        // Create destination with subset of source index settings that can be added before reindex
        var settings = getPreSettings(sourceIndex);

        var sourceMapping = sourceIndex.mapping();
        Map<String, Object> mapping = sourceMapping != null ? sourceMapping.rawSourceAsMap() : Map.of();
        var createIndexRequest = new CreateIndexRequest(destIndexName).settings(settings).mapping(mapping);

        var errorMessage = String.format(Locale.ROOT, "Could not create index [%s]", destIndexName);
        client.admin().indices().create(createIndexRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    private void reindex(String sourceIndexName, String destIndexName, ActionListener<BulkByScrollResponse> listener) {
        logger.info("Reindex to destination index [{}] from source index [{}]", destIndexName, sourceIndexName);
        var reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndexName);
        reindexRequest.getSearchRequest().allowPartialSearchResults(false);
        reindexRequest.getSearchRequest().source().fetchSource(true);
        reindexRequest.setDestIndex(destIndexName);
        client.execute(ReindexAction.INSTANCE, reindexRequest, listener);
    }

    private void updateSettings(Settings settingsBefore, String destIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.info("Adding settings from source index that could not be added before reindex");

        Settings postSettings = getPostSettings(settingsBefore);
        if (postSettings.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        var updateSettingsRequest = new UpdateSettingsRequest(postSettings, destIndexName);
        var errorMessage = String.format(Locale.ROOT, "Could not update settings on index [%s]", destIndexName);
        client.admin().indices().updateSettings(updateSettingsRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    // Filter source index settings to subset of settings that can be included during reindex.
    // Similar to the settings filtering done when reindexing for upgrade in Kibana
    // https://github.com/elastic/kibana/blob/8a8363f02cc990732eb9cbb60cd388643a336bed/x-pack
    // /plugins/upgrade_assistant/server/lib/reindexing/index_settings.ts#L155
    // TODO does tsdb start/end time get set in create request if copied into settings from source index
    private Settings getPreSettings(IndexMetadata sourceIndex) {
        // filter settings that will be added back later
        var filtered = sourceIndex.getSettings().filter(settingName -> SETTINGS_TO_ADD_BACK.contains(settingName) == false);

        // filter private and non-copyable settings
        var builder = MetadataCreateIndexService.copySettingsFromSource(false, filtered, indexScopedSettings, Settings.builder());
        return builder.build();
    }

    private Settings getPostSettings(Settings settingsBefore) {
        return settingsBefore.filter(SETTINGS_TO_ADD_BACK::contains);
    }

    public static String generateDestIndexName(String sourceIndex) {
        return "upgrade-" + sourceIndex;
    }

    private static <U extends AcknowledgedResponse> ActionListener<U> failIfNotAcknowledged(
        ActionListener<U> listener,
        String errorMessage
    ) {
        return listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.isAcknowledged()) {
                delegate.onResponse(null);
            }
            throw new ElasticsearchException(errorMessage);
        });
    }
}
