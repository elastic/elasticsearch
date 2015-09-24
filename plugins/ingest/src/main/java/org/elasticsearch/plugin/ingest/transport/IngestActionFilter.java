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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.PipelineStore;

import java.util.List;
import java.util.Map;

public class IngestActionFilter extends ActionFilter.Simple {

    private final PipelineStore pipelineStore;

    @Inject
    public IngestActionFilter(Settings settings, PipelineStore pipelineStore) {
        super(settings);
        this.pipelineStore = pipelineStore;
    }

    @Override
    protected boolean apply(String action, ActionRequest request, ActionListener listener) {
        String pipelineId = request.getFromContext(IngestPlugin.INGEST_CONTEXT_KEY);
        if (pipelineId == null) {
            pipelineId = request.getHeader(IngestPlugin.INGEST_HTTP_PARAM);
            if (pipelineId == null) {
                return true;
            }
        }
        Pipeline pipeline = pipelineStore.get(pipelineId);
        if (pipeline == null) {
            return true;
        }

        if (request instanceof IndexRequest) {
            processIndexRequest((IndexRequest) request, pipeline);
        } else if (request instanceof BulkRequest) {
            BulkRequest bulkRequest = (BulkRequest) request;
            List<ActionRequest> actionRequests = bulkRequest.requests();
            for (ActionRequest actionRequest : actionRequests) {
                if (actionRequest instanceof IndexRequest) {
                    processIndexRequest((IndexRequest) actionRequest, pipeline);
                }
            }
        }
        return true;
    }

    void processIndexRequest(IndexRequest indexRequest, Pipeline pipeline) {
        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        Data data = new Data(indexRequest.index(), indexRequest.type(), indexRequest.id(), sourceAsMap);
        pipeline.execute(data);
        indexRequest.source(data.getDocument());
    }

    @Override
    protected boolean apply(String action, ActionResponse response, ActionListener listener) {
        return true;
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }
}
