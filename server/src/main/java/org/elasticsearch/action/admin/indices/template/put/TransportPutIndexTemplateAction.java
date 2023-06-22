/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.template.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Put index template action.
 */
public class TransportPutIndexTemplateAction extends AcknowledgedTransportMasterNodeAction<PutIndexTemplateRequest> {

    private static final Logger logger = LogManager.getLogger(TransportPutIndexTemplateAction.class);

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportPutIndexTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter
    ) {
        super(
            PutIndexTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutIndexTemplateRequest::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected ClusterBlockException checkBlock(PutIndexTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        final PutIndexTemplateRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) throws IOException {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }
        final Settings.Builder templateSettingsBuilder = Settings.builder();
        templateSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(templateSettingsBuilder.build(), true); // templates must be consistent with regards to dependencies
        try {
            settingsFilter.validateSettings(request.settings());
        } catch (Exception e) {
            listener.onFailure(e);
        }
        indexTemplateService.putTemplate(
            new MetadataIndexTemplateService.PutRequest(cause, request.name()).patterns(request.patterns())
                .order(request.order())
                .settings(templateSettingsBuilder.build())
                .mappings(request.mappings() == null ? null : new CompressedXContent(request.mappings()))
                .aliases(request.aliases())
                .create(request.create())
                .masterTimeout(request.masterNodeTimeout())
                .version(request.version()),

            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> "failed to put template [" + request.name() + "]", e);
                    listener.onFailure(e);
                }
            }
        );
    }
}
