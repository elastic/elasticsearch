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
import org.elasticsearch.ingest.Data;
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
        String pipelineId = request.getFromContext(IngestPlugin.INGEST_CONTEXT_KEY);
        if (pipelineId == null) {
            pipelineId = request.getHeader(IngestPlugin.INGEST_PARAM);
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
        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        Data data = new Data(indexRequest.index(), indexRequest.type(), indexRequest.id(), sourceAsMap);
        executionService.execute(data, pipelineId, new PipelineExecutionService.Listener() {
            @Override
            public void executed(Data data) {
                if (data.isModified()) {
                    indexRequest.source(data.getDocument());
                }
                chain.proceed(action, indexRequest, listener);
            }

            @Override
            public void failed(Exception e) {
                logger.error("failed to execute pipeline [{}]", e, pipelineId);
                listener.onFailure(e);
            }
        });
    }

    // TODO: rethink how to deal with bulk requests:
    // This doesn't scale very well for a single bulk requests, so it would be great if a bulk requests could be broken up into several chunks so that the ingesting can be paralized
    // on the other hand if there are many index/bulk requests then breaking up bulk requests isn't going to help much.
    // I think the execution service should be smart enough about when it should break things up in chunks based on the ingest threadpool usage,
    // this means that the contract of the execution service should change in order to accept multiple data instances.
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
        Data data = new Data(indexRequest.index(), indexRequest.type(), indexRequest.id(), sourceAsMap);
        executionService.execute(data, pipelineId, new PipelineExecutionService.Listener() {
            @Override
            public void executed(Data data) {
                if (data.isModified()) {
                    indexRequest.source(data.getDocument());
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
