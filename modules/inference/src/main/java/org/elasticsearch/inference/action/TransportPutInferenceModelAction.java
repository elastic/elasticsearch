/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.registry.ModelRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Map;

public class TransportPutInferenceModelAction extends TransportMasterNodeAction<PutInferenceModelAction.Request, AcknowledgedResponse> {

    private final ModelRegistry modelRegistry;

    @Inject
    public TransportPutInferenceModelAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ModelRegistry modelRegistry
    ) {
        super(
            PutInferenceModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutInferenceModelAction.Request::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            ThreadPool.Names.SAME
        );
        this.modelRegistry = modelRegistry;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutInferenceModelAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {

        var requestAsMap = requestToMap(request);
        String service = (String) requestAsMap.remove(Model.SERVICE);


        request.getContentType()
        this.modelRegistry.storeModel();
        listener.onResponse(AcknowledgedResponse.TRUE);
    }

    private Map<String, Object> requestToMap(PutInferenceModelAction.Request request) throws IOException {
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY;
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                request.getContent(),
                request.getContentType()
            )
        ) {
            return parser.map();
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutInferenceModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
