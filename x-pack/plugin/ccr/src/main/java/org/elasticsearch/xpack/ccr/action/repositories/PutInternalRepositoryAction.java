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

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class PutInternalRepositoryAction extends Action<ActionResponse> {

    public static final PutInternalRepositoryAction INSTANCE = new PutInternalRepositoryAction();
    public static final String NAME = "cluster:admin/internal_repository/put";

    private PutInternalRepositoryAction() {
        super(NAME);
    }

    @Override
    public ActionResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<ActionResponse> getResponseReader() {
        return in -> new ActionResponse() {};
    }

    public static class TransportPutInternalRepositoryAction extends TransportAction<PutInternalRepositoryRequest, ActionResponse> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportPutInternalRepositoryAction(RepositoriesService repositoriesService, ActionFilters actionFilters,
                                                    TransportService transportService) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(Task task, PutInternalRepositoryRequest request, ActionListener<ActionResponse> listener) {
            repositoriesService.registerInternalRepository(request.getName(), request.getType(), request.getSettings());
            listener.onResponse(new ActionResponse() {});
        }
    }
}
