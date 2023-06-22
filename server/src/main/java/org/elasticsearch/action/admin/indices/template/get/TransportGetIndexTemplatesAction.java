/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransportGetIndexTemplatesAction extends TransportMasterNodeReadAction<GetIndexTemplatesRequest, GetIndexTemplatesResponse> {

    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetIndexTemplatesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SettingsFilter settingsFilter
    ) {
        super(
            GetIndexTemplatesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetIndexTemplatesRequest::new,
            indexNameExpressionResolver,
            GetIndexTemplatesResponse::new,
            ThreadPool.Names.SAME
        );
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexTemplatesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        GetIndexTemplatesRequest request,
        ClusterState state,
        ActionListener<GetIndexTemplatesResponse> listener
    ) {
        List<IndexTemplateMetadata> results;

        // If we did not ask for a specific name, then we return all templates
        if (request.names().length == 0) {
            results = new ArrayList<>(state.metadata().templates().values());
        } else {
            results = new ArrayList<>();
        }

        for (String name : request.names()) {
            if (Regex.isSimpleMatchPattern(name)) {
                for (Map.Entry<String, IndexTemplateMetadata> entry : state.metadata().templates().entrySet()) {
                    if (Regex.simpleMatch(name, entry.getKey())) {

                        IndexTemplateMetadata value = entry.getValue();
                        Settings filter = settingsFilter.filter(value.settings());

                        // maybe modify? or change toXcontent?
                        results.add(
                            new IndexTemplateMetadata(
                                value.name(),
                                value.order(),
                                value.version(),
                                value.patterns(),
                                filter,
                                value.mappings,
                                value.aliases()
                            )
                        );
                    }
                }
            } else if (state.metadata().templates().containsKey(name)) {
                results.add(state.metadata().templates().get(name));
            }
        }
        listener.onResponse(new GetIndexTemplatesResponse(results));
    }
}
