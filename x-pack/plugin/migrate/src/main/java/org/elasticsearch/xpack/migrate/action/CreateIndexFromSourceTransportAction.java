/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CreateIndexFromSourceTransportAction extends HandledTransportAction<
    CreateIndexFromSourceAction.Request,
    AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(CreateIndexFromSourceTransportAction.class);

    private final ClusterService clusterService;
    private final Client client;
    private final IndexScopedSettings indexScopedSettings;
    private static final Set<String> INDEX_BLOCK_SETTINGS = Set.of(
        IndexMetadata.SETTING_READ_ONLY,
        IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE,
        IndexMetadata.SETTING_BLOCKS_WRITE,
        IndexMetadata.SETTING_BLOCKS_METADATA,
        IndexMetadata.SETTING_BLOCKS_READ
    );

    @Inject
    public CreateIndexFromSourceTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client,
        IndexScopedSettings indexScopedSettings
    ) {
        super(
            CreateIndexFromSourceAction.NAME,
            false,
            transportService,
            actionFilters,
            CreateIndexFromSourceAction.Request::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.clusterService = clusterService;
        this.client = client;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected void doExecute(Task task, CreateIndexFromSourceAction.Request request, ActionListener<AcknowledgedResponse> listener) {

        IndexMetadata sourceIndex = clusterService.state().getMetadata().getProject().index(request.sourceIndex());

        if (sourceIndex == null) {
            listener.onFailure(new IndexNotFoundException(request.sourceIndex()));
            return;
        }

        logger.debug("Creating destination index [{}] for source index [{}]", request.destIndex(), request.sourceIndex());

        Settings.Builder settings = Settings.builder()
            // first settings from source index
            .put(filterSettings(sourceIndex));

        if (request.settingsOverride().isEmpty() == false) {
            applyOverrides(settings, request.settingsOverride());
        }

        if (request.removeIndexBlocks()) {
            // lastly, override with settings to remove index blocks if requested
            INDEX_BLOCK_SETTINGS.forEach(settings::remove);
        }

        Map<String, Object> mergeMappings;
        try {
            mergeMappings = mergeMappings(sourceIndex.mapping(), request.mappingsOverride());
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        var createIndexRequest = new CreateIndexRequest(request.destIndex()).settings(settings);
        createIndexRequest.cause("create-index-from-source");
        if (mergeMappings.isEmpty() == false) {
            createIndexRequest.mapping(mergeMappings);
        }
        createIndexRequest.setParentTask(new TaskId(clusterService.localNode().getId(), task.getId()));

        client.admin().indices().create(createIndexRequest, listener.map(response -> response));
    }

    private void applyOverrides(Settings.Builder settings, Settings overrides) {
        overrides.keySet().forEach(key -> {
            if (overrides.get(key) != null) {
                settings.put(key, overrides.get(key));
            } else {
                settings.remove(key);
            }
        });
    }

    private static Map<String, Object> toMap(@Nullable MappingMetadata sourceMapping) {
        return Optional.ofNullable(sourceMapping)
            .map(MappingMetadata::source)
            .map(CompressedXContent::uncompressed)
            .map(s -> XContentHelper.convertToMap(s, true, XContentType.JSON).v2())
            .orElse(Map.of());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mergeMappings(@Nullable MappingMetadata sourceMapping, Map<String, Object> mappingAddition)
        throws IOException {
        Map<String, Object> combinedMappingMap = new HashMap<>(toMap(sourceMapping));
        XContentHelper.update(combinedMappingMap, mappingAddition, true);
        if (sourceMapping != null && combinedMappingMap.size() == 1 && combinedMappingMap.containsKey(sourceMapping.type())) {
            combinedMappingMap = (Map<String, Object>) combinedMappingMap.get(sourceMapping.type());
        }
        return combinedMappingMap;
    }

    // Filter source index settings to subset of settings that can be included during reindex.
    // Similar to the settings filtering done when reindexing for upgrade in Kibana
    // https://github.com/elastic/kibana/blob/8a8363f02cc990732eb9cbb60cd388643a336bed/x-pack
    // /plugins/upgrade_assistant/server/lib/reindexing/index_settings.ts#L155
    private Settings filterSettings(IndexMetadata sourceIndex) {
        Settings sourceSettings = sourceIndex.getSettings();
        final Settings.Builder builder = Settings.builder();
        for (final String key : sourceSettings.keySet()) {
            final Setting<?> setting = indexScopedSettings.get(key);
            if (setting == null) {
                assert indexScopedSettings.isPrivateSetting(key) : key;
                continue;
            }
            if (setting.isPrivateIndex()) {
                continue;
            }
            if (setting.getProperties().contains(Setting.Property.NotCopyableOnResize)) {
                continue;
            }
            if (setting.getProperties().contains(Setting.Property.IndexSettingDeprecatedInV7AndRemovedInV8)) {
                continue;
            }
            if (SPECIFIC_SETTINGS_TO_REMOVE.contains(key)) {
                continue;
            }
            builder.copy(key, sourceSettings);
        }
        return builder.build();
    }

    private static final Set<String> SPECIFIC_SETTINGS_TO_REMOVE = Set.of(
        /**
         * These 3 settings were removed from indices created by UA in https://github.com/elastic/kibana/pull/93293
         * That change only removed `index.translog.retention.size` and `index.translog.retention.age` if
         * soft deletes were enabled. Since soft deletes are always enabled in v8, we can remove all three settings.
         */
        IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(),
        IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(),
        IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey()
    );
}
