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
package org.elasticsearch.action.template.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportGetSearchTemplatesAction extends TransportMasterNodeReadOperationAction<GetSearchTemplatesRequest, GetSearchTemplatesResponse> {

    @Inject
    public TransportGetSearchTemplatesAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override
    protected String transportAction() {
        return GetSearchTemplatesAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSearchTemplatesRequest newRequest() {
        return new GetSearchTemplatesRequest();
    }

    @Override
    protected GetSearchTemplatesResponse newResponse() {
        return new GetSearchTemplatesResponse();
    }

    @Override
    protected void masterOperation(GetSearchTemplatesRequest request, ClusterState state, ActionListener<GetSearchTemplatesResponse> listener) throws ElasticsearchException {
        throw new ElasticsearchException("FOO BAR");
        /*
        List<IndexTemplateMetaData> results;

        // If we did not ask for a specific name, then we return all templates
        if (request.names().length == 0) {
            results = Lists.newArrayList(state.metaData().templates().values().toArray(IndexTemplateMetaData.class));
        } else {
            results = Lists.newArrayList();
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

        listener.onResponse(new GetSearchTemplatesResponse(results));
        */
    }
}
