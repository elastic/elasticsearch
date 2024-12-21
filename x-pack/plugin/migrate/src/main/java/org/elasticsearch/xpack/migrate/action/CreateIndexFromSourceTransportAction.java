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
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
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

public class CreateIndexFromSourceTransportAction extends HandledTransportAction<
    CreateIndexFromSourceAction.Request,
    AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(CreateIndexFromSourceTransportAction.class);

    private final ClusterService clusterService;
    private final Client client;
    private final IndexScopedSettings indexScopedSettings;

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

        IndexMetadata sourceIndex = clusterService.state().getMetadata().index(request.getSourceIndex());

        if (sourceIndex == null) {
            listener.onFailure(new IndexNotFoundException(request.getSourceIndex()));
            return;
        }

        logger.debug("Creating destination index [{}] for source index [{}]", request.getDestIndex(), request.getSourceIndex());

        Settings settings = Settings.builder()
            // add source settings
            .put(filterSettings(sourceIndex))
            // add override settings from request
            .put(request.getSettingsOverride())
            .build();

        Map<String, Object> mergeMappings;
        try {
            mergeMappings = mergeMappings(sourceIndex.mapping(), request.getMappingsOverride());
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        var createIndexRequest = new CreateIndexRequest(request.getDestIndex()).settings(settings);
        if (mergeMappings.isEmpty() == false) {
            createIndexRequest.mapping(mergeMappings);
        }
        createIndexRequest.setParentTask(new TaskId(clusterService.localNode().getId(), task.getId()));

        client.admin().indices().create(createIndexRequest, listener.map(response -> response));
    }

    private static Map<String, Object> toMap(@Nullable MappingMetadata sourceMapping) {
        return Optional.ofNullable(sourceMapping)
            .map(MappingMetadata::source)
            .map(CompressedXContent::uncompressed)
            .map(s -> XContentHelper.convertToMap(s, true, XContentType.JSON).v2())
            .orElse(Map.of());
    }

    private static Map<String, Object> mergeMappings(@Nullable MappingMetadata sourceMapping, Map<String, Object> mappingAddition)
        throws IOException {
        Map<String, Object> combinedMappingMap = new HashMap<>(toMap(sourceMapping));
        XContentHelper.update(combinedMappingMap, mappingAddition, true);
        return combinedMappingMap;
    }

    // Filter source index settings to subset of settings that can be included during reindex.
    // Similar to the settings filtering done when reindexing for upgrade in Kibana
    // https://github.com/elastic/kibana/blob/8a8363f02cc990732eb9cbb60cd388643a336bed/x-pack
    // /plugins/upgrade_assistant/server/lib/reindexing/index_settings.ts#L155
    private Settings filterSettings(IndexMetadata sourceIndex) {
        return MetadataCreateIndexService.copySettingsFromSource(false, sourceIndex.getSettings(), indexScopedSettings, Settings.builder())
            .build();
    }
}
