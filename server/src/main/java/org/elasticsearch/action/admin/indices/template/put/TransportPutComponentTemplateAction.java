/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPutComponentTemplateAction
    extends AcknowledgedTransportMasterNodeAction<PutComponentTemplateAction.Request> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public TransportPutComponentTemplateAction(TransportService transportService, ClusterService clusterService,
                                               ThreadPool threadPool, MetadataIndexTemplateService indexTemplateService,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                               IndexScopedSettings indexScopedSettings) {
        super(PutComponentTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutComponentTemplateAction.Request::new, indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected ClusterBlockException checkBlock(PutComponentTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, final PutComponentTemplateAction.Request request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        ComponentTemplate componentTemplate = request.componentTemplate();
        Template template = componentTemplate.template();
        // Normalize the index settings if necessary
        if (template.settings() != null) {
            Settings.Builder builder = Settings.builder().put(template.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
            Settings settings = builder.build();
            indexScopedSettings.validate(settings, true);
            template = new Template(settings, template.mappings(), template.aliases());
            componentTemplate = new ComponentTemplate(template, componentTemplate.version(), componentTemplate.metadata());
        }
        indexTemplateService.putComponentTemplate(request.cause(), request.create(), request.name(), request.masterNodeTimeout(),
            componentTemplate, listener);
    }
}
