/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class TransportGetComponentTemplateAction extends
    TransportMasterNodeReadAction<GetComponentTemplateAction.Request, GetComponentTemplateAction.Response> {

    @Inject
    public TransportGetComponentTemplateAction(TransportService transportService, ClusterService clusterService,
                                               ThreadPool threadPool, ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetComponentTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
                GetComponentTemplateAction.Request::new, indexNameExpressionResolver, GetComponentTemplateAction.Response::new,
                ThreadPool.Names.SAME);
    }

    @Override
    protected ClusterBlockException checkBlock(GetComponentTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(Task task, GetComponentTemplateAction.Request request, ClusterState state,
                                   ActionListener<GetComponentTemplateAction.Response> listener) {
        Map<String, ComponentTemplate> allTemplates = state.metadata().componentTemplates();

        // If we did not ask for a specific name, then we return all templates
        if (request.name() == null) {
            listener.onResponse(new GetComponentTemplateAction.Response(allTemplates));
            return;
        }

        final Map<String, ComponentTemplate> results = new HashMap<>();
        String name = request.name();
        if (Regex.isSimpleMatchPattern(name)) {
            for (Map.Entry<String, ComponentTemplate> entry : allTemplates.entrySet()) {
                if (Regex.simpleMatch(name, entry.getKey())) {
                    results.put(entry.getKey(), entry.getValue());
                }
            }
        } else if (allTemplates.containsKey(name)) {
            results.put(name, allTemplates.get(name));
        } else {
            throw new ResourceNotFoundException("component template matching [" + request.name() + "] not found");
        }

        listener.onResponse(new GetComponentTemplateAction.Response(results));
    }
}
