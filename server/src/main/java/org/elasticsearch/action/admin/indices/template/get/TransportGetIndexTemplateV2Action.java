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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportGetIndexTemplateV2Action
    extends TransportMasterNodeReadAction<GetIndexTemplateV2Action.Request, GetIndexTemplateV2Action.Response> {

    @Inject
    public TransportGetIndexTemplateV2Action(TransportService transportService, ClusterService clusterService,
                                               ThreadPool threadPool, ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetIndexTemplateV2Action.NAME, transportService, clusterService, threadPool, actionFilters,
            GetIndexTemplateV2Action.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetIndexTemplateV2Action.Response read(StreamInput in) throws IOException {
        return new GetIndexTemplateV2Action.Response(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexTemplateV2Action.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(Task task, GetIndexTemplateV2Action.Request request, ClusterState state,
                                   ActionListener<GetIndexTemplateV2Action.Response> listener) {
        Map<String, IndexTemplateV2> allTemplates = state.metadata().templatesV2();

        // If we did not ask for a specific name, then we return all templates
        if (request.name() == null) {
            listener.onResponse(new GetIndexTemplateV2Action.Response(allTemplates));
            return;
        }

        final Map<String, IndexTemplateV2> results = new HashMap<>();
        String name = request.name();
        if (Regex.isSimpleMatchPattern(name)) {
            for (Map.Entry<String, IndexTemplateV2> entry : allTemplates.entrySet()) {
                if (Regex.simpleMatch(name, entry.getKey())) {
                    results.put(entry.getKey(), entry.getValue());
                }
            }
        } else if (allTemplates.containsKey(name)) {
            results.put(name, allTemplates.get(name));
        } else {
            throw new ResourceNotFoundException("index template matching [" + request.name() + "] not found");
        }

        listener.onResponse(new GetIndexTemplateV2Action.Response(results));
    }
}
