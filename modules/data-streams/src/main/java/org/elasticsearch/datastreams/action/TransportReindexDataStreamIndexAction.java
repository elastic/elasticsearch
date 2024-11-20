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
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
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
import java.util.stream.Collectors;

public class TransportReindexDataStreamIndexAction extends HandledTransportAction<
    ReindexDataStreamIndexAction.Request,
    ReindexDataStreamIndexAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportReindexDataStreamIndexAction.class);


    private static final Map<String, Setting<?>> INDEX_SETTING_LOOKUP = IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.stream()
        .collect(Collectors.toMap(Setting::getKey, s -> s));

    // copied from
    // https://github.com/elastic/kibana/blob/8a8363f02cc990732eb9cbb60cd388643a336bed/x-pack
    // /plugins/upgrade_assistant/server/lib/reindexing/index_settings.ts#L155
    private static final Set<String> DISALLOWED_SETTINGS = Set.of(
        // Remove write block so can reindex
        IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(),

        // Private ES settings
        "index.allocation.existing_shards_allocator",
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
        "index.version.created"
    );

    // Perhaps replace with string constant, since settings will be removed themselves in v9
    // These settings are listed as deprecated, but do not have the
    private static final Set<String> DEPRECATED_SETTINGS = Set.of(
        IndexMetadata.SETTING_VERSION_UPGRADED,
        IndexMetadata.SETTING_VERSION_UPGRADED_STRING
    );

    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);
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

        var sourceSettingsBefore = sourceIndex.getSettings();

        SubscribableListener.<AcknowledgedResponse>newForked(l -> setReadOnly(sourceIndexName, l))
            .<AcknowledgedResponse>andThen(l -> deleteDestIfExists(destIndexName, l))
            .<CreateIndexResponse>andThen(l -> createIndex(sourceSettingsBefore, sourceIndex, destIndexName, l))
            .<BulkByScrollResponse>andThen(l -> reindex(sourceIndexName, destIndexName, l))
            .<AcknowledgedResponse>andThen(l -> updateSettings(sourceIndex, destIndexName, l))
            // .<AcknowledgedResponse>andThen(l -> createSnapshot(sourceIndex, sourceIndexName, destIndexName, l))
            .andThenApply(ignored -> new ReindexDataStreamIndexAction.Response(destIndexName))
            .addListener(listener);
    }

    private void setReadOnly(String sourceIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.info("Setting source index [{}] to read-only", sourceIndexName);
        final Settings readOnlySettings = Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build();
        var updateSettingsRequest = new UpdateSettingsRequest(readOnlySettings, sourceIndexName);
        var errorMessage = String.format(Locale.ROOT, "Could not set read-only on source index [%s]", sourceIndexName);
        client.admin().indices().updateSettings(updateSettingsRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    private void deleteDestIfExists(String destIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.info("Attempting to delete index [{}]", destIndexName);
        var deleteIndexRequest = new DeleteIndexRequest(destIndexName).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        var errorMessage = String.format(Locale.ROOT, "Failed to acknowledge delete of index [%s]", destIndexName);
        client.admin().indices().delete(deleteIndexRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    private void createIndex(Settings sourceSettingsBefore, IndexMetadata sourceIndex, String destIndexName, ActionListener<CreateIndexResponse> listener) {
        logger.info("Creating destination index [{}] for source index [{}]", destIndexName, sourceIndex.getIndex().getName());

        // Create destination with subset of source index settings that can be added before reindex
        var settings = getPreSettings(sourceSettingsBefore);

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

    private void updateSettings(IndexMetadata sourceIndex, String destIndexName, ActionListener<AcknowledgedResponse> listener) {
        logger.info("Adding settings from source index that could not be added before reindex");

        Settings postSettings = getPostSettings(sourceIndex);
        if (postSettings.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        var updateSettingsRequest = new UpdateSettingsRequest(postSettings, destIndexName);
        var errorMessage = String.format(Locale.ROOT, "Could not update settings on index [%s]", destIndexName);
        client.admin().indices().updateSettings(updateSettingsRequest, failIfNotAcknowledged(listener, errorMessage));
    }

    // Filter source index settings to subset of settings that can be included during reindex
    // TODO does tsdb start/end time get set in create request if copied into settings from source index
    private Settings getPreSettings(Settings settings) {
        return settings
            // Remove settings with deprecated property
            .filter(name -> settingHasProperty(name, Setting.Property.IndexSettingDeprecatedInV7AndRemovedInV8) == false)

            // Remove deprecated settings that don't have deprecated property
            .filter(name -> DEPRECATED_SETTINGS.contains(name) == false)

            // Remove private settings
            .filter(name -> settingHasProperty(name, Setting.Property.PrivateIndex) == false)

            // Remove not copyable settings
            .filter(name -> settingHasProperty(name, Setting.Property.NotCopyableOnResize) == false);
    }

    private boolean settingHasProperty(String name, Setting.Property property) {
        Setting<?> setting = INDEX_SETTING_LOOKUP.get(name);
        if (setting == null) {
            return false;
        }
        return setting.getProperties().contains(property);
    }

    private Settings getPostSettings(IndexMetadata sourceIndex) {
        // TODO
        return Settings.EMPTY;
        // return sourceIndex.getSettings().filter(DISALLOWED_SETTINGS::contains);
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
