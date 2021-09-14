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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPutComposableIndexTemplateAction
    extends AcknowledgedTransportMasterNodeAction<PutComposableIndexTemplateAction.Request> {

    private final MetadataIndexTemplateService indexTemplateService;

    @Inject
    public TransportPutComposableIndexTemplateAction(TransportService transportService, ClusterService clusterService,
                                                     ThreadPool threadPool, MetadataIndexTemplateService indexTemplateService,
                                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(PutComposableIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutComposableIndexTemplateAction.Request::new, indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.indexTemplateService = indexTemplateService;
    }

    @Override
    protected ClusterBlockException checkBlock(PutComposableIndexTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, final PutComposableIndexTemplateAction.Request request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        ComposableIndexTemplate indexTemplate = request.indexTemplate();
        indexTemplateService.putIndexTemplateV2(request.cause(), request.create(), request.name(), request.masterNodeTimeout(),
            indexTemplate, listener);
    }
}
