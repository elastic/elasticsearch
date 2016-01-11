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
package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.Strings;
import org.elasticsearch.tasks.Task;

public final class IngestDisabledActionFilter implements ActionFilter {

    @Override
    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        boolean isIngestRequest = false;
        if (IndexAction.NAME.equals(action)) {
            assert request instanceof IndexRequest;
            IndexRequest indexRequest = (IndexRequest) request;
            isIngestRequest = Strings.hasText(indexRequest.pipeline());
        } else if (BulkAction.NAME.equals(action)) {
            assert request instanceof BulkRequest;
            BulkRequest bulkRequest = (BulkRequest) request;
            for (ActionRequest actionRequest : bulkRequest.requests()) {
                if (actionRequest instanceof IndexRequest) {
                    IndexRequest indexRequest = (IndexRequest) actionRequest;
                    if (Strings.hasText(indexRequest.pipeline())) {
                        isIngestRequest = true;
                        break;
                    }
                }
            }
        }
        if (isIngestRequest) {
            throw new IllegalArgumentException("node.ingest is set to false, cannot execute pipeline");
        }
        chain.proceed(task, action, request, listener);
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }
}
