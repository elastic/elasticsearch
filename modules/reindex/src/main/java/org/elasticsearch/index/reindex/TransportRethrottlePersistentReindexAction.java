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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParamsUpdateFunction;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportRethrottlePersistentReindexAction extends
    HandledTransportAction<RethrottlePersistentReindexAction.Request, RethrottlePersistentReindexAction.Response> {

    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportRethrottlePersistentReindexAction(TransportService transportService,ActionFilters actionFilters,
                                                      PersistentTasksService persistentTasksService) {
        super(RethrottlePersistentReindexAction.NAME, transportService, actionFilters, RethrottlePersistentReindexAction.Request::new);
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, RethrottlePersistentReindexAction.Request request,
                             ActionListener<RethrottlePersistentReindexAction.Response> listener) {
        persistentTasksService.sendUpdateParamsRequest(request.getPersistentTaskId(),
            new RethrottlePersistentTaskFunction(request.getRequestsPerSecond()),
            ActionListener.map(listener, v -> new RethrottlePersistentReindexAction.Response()));
    }

    public static class RethrottlePersistentTaskFunction implements PersistentTaskParamsUpdateFunction<ReindexTaskParams> {
        public static final String NAME = "rethrottle-reindex";
        private float requestsPerSecond;

        public RethrottlePersistentTaskFunction(float requestsPerSecond) {
            this.requestsPerSecond = requestsPerSecond;
        }

        public RethrottlePersistentTaskFunction(StreamInput in) throws IOException {
            this.requestsPerSecond = in.readFloat();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeFloat(requestsPerSecond);
        }

        @Override
        public ReindexTaskParams apply(ReindexTaskParams params) {
            return new ReindexTaskParams(params.shouldStoreResult(), params.getHeaders(), requestsPerSecond);
        }
    }
}
