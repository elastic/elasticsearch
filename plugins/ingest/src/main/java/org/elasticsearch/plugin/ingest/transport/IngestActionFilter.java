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
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.PipelineExecutionService;

import java.util.Iterator;
import java.util.Map;

public class IngestActionFilter extends AbstractComponent implements ActionFilter {

    private final PipelineExecutionService executionService;

    @Inject
    public IngestActionFilter(Settings settings, PipelineExecutionService executionService) {
        super(settings);
        this.executionService = executionService;
    }

    @Override
    public void apply(String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        String pipelineId = request.getFromContext(IngestPlugin.PIPELINE_ID_PARAM_CONTEXT_KEY);
        if (pipelineId == null) {
            pipelineId = request.getHeader(IngestPlugin.PIPELINE_ID_PARAM);
            if (pipelineId == null) {
                chain.proceed(action, request, listener);
                return;
            }
        }

        if (request instanceof IndexRequest) {
            processIndexRequest(action, listener, chain, (IndexRequest) request, pipelineId);
        } else if (request instanceof BulkRequest) {
            BulkRequest bulkRequest = (BulkRequest) request;
            processBulkIndexRequest(action, listener, chain, bulkRequest, pipelineId, bulkRequest.requests().iterator());
        } else {
            chain.proceed(action, request, listener);
        }
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

    void processIndexRequest(String action, ActionListener listener, ActionFilterChain chain, IndexRequest indexRequest, String pipelineId) {
        // The IndexRequest has the same type on the node that receives the request and the node that
        // processes the primary action. This could lead to a pipeline being executed twice for the same
        // index request, hence this check
        if (indexRequest.hasHeader(IngestPlugin.PIPELINE_ALREADY_PROCESSED)) {
            chain.proceed(action, indexRequest, listener);
            return;
        }

        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        IngestDocument ingestDocument = new IngestDocument(indexRequest.index(), indexRequest.type(), indexRequest.id(), sourceAsMap);
        executionService.execute(ingestDocument, pipelineId, new PipelineExecutionService.Listener() {
            @Override
            public void executed(IngestDocument ingestDocument) {
                if (ingestDocument.isModified()) {
                    indexRequest.source(ingestDocument.getSource());
                }
                indexRequest.putHeader(IngestPlugin.PIPELINE_ALREADY_PROCESSED, true);
                chain.proceed(action, indexRequest, listener);
            }

            @Override
            public void failed(Exception e) {
                logger.error("failed to execute pipeline [{}]", e, pipelineId);
                listener.onFailure(e);
            }
        });
    }

    void processBulkIndexRequest(String action, ActionListener listener, ActionFilterChain chain, BulkRequest bulkRequest, String pipelineId, Iterator<ActionRequest> requests) {
        if (!requests.hasNext()) {
            chain.proceed(action, bulkRequest, listener);
            return;
        }

        ActionRequest actionRequest = requests.next();
        if (!(actionRequest instanceof IndexRequest)) {
            processBulkIndexRequest(action, listener, chain, bulkRequest, pipelineId, requests);
            return;
        }

        IndexRequest indexRequest = (IndexRequest) actionRequest;
        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        IngestDocument ingestDocument = new IngestDocument(indexRequest.index(), indexRequest.type(), indexRequest.id(), sourceAsMap);
        executionService.execute(ingestDocument, pipelineId, new PipelineExecutionService.Listener() {
            @Override
            public void executed(IngestDocument ingestDocument) {
                if (ingestDocument.isModified()) {
                    indexRequest.source(ingestDocument.getSource());
                }
                processBulkIndexRequest(action, listener, chain, bulkRequest, pipelineId, requests);
            }

            @Override
            public void failed(Exception e) {
                logger.error("failed to execute pipeline [{}]", e, pipelineId);
                listener.onFailure(e);
            }
        });
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }
}
