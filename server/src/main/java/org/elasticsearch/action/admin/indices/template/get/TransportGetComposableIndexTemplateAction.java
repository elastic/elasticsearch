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
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class TransportGetComposableIndexTemplateAction
    extends TransportMasterNodeReadAction<GetComposableIndexTemplateAction.Request, GetComposableIndexTemplateAction.Response> {

    @Inject
    public TransportGetComposableIndexTemplateAction(TransportService transportService, ClusterService clusterService,
                                                     ThreadPool threadPool, ActionFilters actionFilters,
                                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetComposableIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
                GetComposableIndexTemplateAction.Request::new, indexNameExpressionResolver,
                GetComposableIndexTemplateAction.Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected ClusterBlockException checkBlock(GetComposableIndexTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(Task task, GetComposableIndexTemplateAction.Request request, ClusterState state,
                                   ActionListener<GetComposableIndexTemplateAction.Response> listener) {
        final Map<String, ComposableIndexTemplate> allTemplates = state.metadata().templatesV2();
        final String[] requestedNames = request.names();

        // If we did not ask for a specific name, then we return all templates
        if (requestedNames.length == 0) {
            listener.onResponse(new GetComposableIndexTemplateAction.Response(allTemplates));
            return;
        }

        boolean hasWildcards = false;
        final Map<String, ComposableIndexTemplate> results = new HashMap<>();
        for (String requestedName : requestedNames) {
            if (Regex.isMatchAllPattern(requestedName)) {
                listener.onResponse(new GetComposableIndexTemplateAction.Response(allTemplates));
                return;
            } else if (Regex.isSimpleMatchPattern(requestedName)) {
                hasWildcards = true;
                for (Map.Entry<String, ComposableIndexTemplate> entry : allTemplates.entrySet()) {
                    if (Regex.simpleMatch(requestedName, entry.getKey())) {
                        results.put(entry.getKey(), entry.getValue());
                    }
                }
            } else if (allTemplates.containsKey(requestedName)) {
                results.put(requestedName, allTemplates.get(requestedName));
            }
        }

        if (hasWildcards == false && results.isEmpty()) {
            throw new ResourceNotFoundException(
                "index templates [" + Strings.arrayToCommaDelimitedString(requestedNames) + "] not found"
            );
        }

        listener.onResponse(new GetComposableIndexTemplateAction.Response(results));
    }
}
