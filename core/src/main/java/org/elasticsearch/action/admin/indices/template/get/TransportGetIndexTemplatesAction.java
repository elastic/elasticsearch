/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.template.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class TransportGetIndexTemplatesAction extends TransportMasterNodeReadAction<GetIndexTemplatesRequest, GetIndexTemplatesResponse> {

    @Inject
    public TransportGetIndexTemplatesAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetIndexTemplatesAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, GetIndexTemplatesRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexTemplatesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected GetIndexTemplatesResponse newResponse() {
        return new GetIndexTemplatesResponse();
    }

    @Override
    protected void masterOperation(GetIndexTemplatesRequest request, ClusterState state, ActionListener<GetIndexTemplatesResponse> listener) {
        List<IndexTemplateMetaData> results;

        // If we did not ask for a specific name, then we return all templates
        if (request.names().length == 0) {
            results = Arrays.asList(state.metaData().templates().values().toArray(IndexTemplateMetaData.class));
        } else {
            results = new ArrayList<>();
        }

        for (String name : request.names()) {
            if (Regex.isSimpleMatchPattern(name)) {
                for (ObjectObjectCursor<String, IndexTemplateMetaData> entry : state.metaData().templates()) {
                    if (Regex.simpleMatch(name, entry.key)) {
                        results.add(entry.value);
                    }
                }
            } else if (state.metaData().templates().containsKey(name)) {
                results.add(state.metaData().templates().get(name));
            }
        }

        listener.onResponse(new GetIndexTemplatesResponse(results));
    }
}
