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
package org.elasticsearch.plugin.ingest.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.plugin.ingest.IngestPlugin;

public final class IngestDisabledActionFilter implements ActionFilter {

    @Override
    public void apply(String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        String pipelineId = request.getFromContext(IngestPlugin.PIPELINE_ID_PARAM_CONTEXT_KEY);
        if (pipelineId != null) {
            failRequest(pipelineId);
        }
        pipelineId = request.getHeader(IngestPlugin.PIPELINE_ID_PARAM);
        if (pipelineId != null) {
            failRequest(pipelineId);
        }

        chain.proceed(action, request, listener);
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    private static void failRequest(String pipelineId) {
        throw new IllegalArgumentException("ingest plugin is disabled, cannot execute pipeline with id [" + pipelineId + "]");
    }

}
