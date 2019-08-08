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

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportGetStoredScriptsAction extends TransportMasterNodeReadAction<GetStoredScriptsRequest,
        GetStoredScriptsResponse> {

    private final ScriptService scriptService;

    @Inject
    public TransportGetStoredScriptsAction(TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver, ScriptService scriptService) {
        super(GetStoredScriptsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetStoredScriptsRequest::new, indexNameExpressionResolver);
        this.scriptService = scriptService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetStoredScriptsResponse read(StreamInput in) throws IOException {
        return new GetStoredScriptsResponse(in);
    }

    @Override
    protected void masterOperation(Task task, GetStoredScriptsRequest request, ClusterState state,
                                   ActionListener<GetStoredScriptsResponse> listener) throws Exception {

        Map<String, StoredScriptSource> results;

        Map<String, StoredScriptSource> storedScripts = scriptService.getStoredScripts(state);
        // If we did not ask for a specific name, then we return all templates
        if (request.names().length == 0) {
            results = storedScripts;
        } else {
            results = new HashMap<>();
        }

        if (storedScripts != null) {
            for (String name : request.names()) {
                if (Regex.isSimpleMatchPattern(name)) {
                    for (Map.Entry<String, StoredScriptSource> entry : storedScripts.entrySet()) {
                        if (Regex.simpleMatch(name, entry.getKey())) {
                            results.put(entry.getKey(), entry.getValue());
                        }
                    }
                } else if (storedScripts.containsKey(name)) {
                    results.put(name, storedScripts.get(name));
                }
            }
        }

        listener.onResponse(new GetStoredScriptsResponse(results));
    }

    @Override
    protected ClusterBlockException checkBlock(GetStoredScriptsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
