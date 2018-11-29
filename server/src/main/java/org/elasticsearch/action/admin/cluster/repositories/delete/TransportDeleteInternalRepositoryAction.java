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

package org.elasticsearch.action.admin.cluster.repositories.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteInternalRepositoryAction extends TransportAction<DeleteInternalRepositoryRequest, AcknowledgedResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportDeleteInternalRepositoryAction(RepositoriesService repositoriesService, ActionFilters actionFilters,
                                                   TransportService transportService) {
        super(DeleteInternalRepositoryAction.NAME, actionFilters, transportService.getTaskManager());
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected void doExecute(Task task, DeleteInternalRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
        repositoriesService.unregisterInternalRepository(request.getName());
        listener.onResponse(new AcknowledgedResponse(true));
    }
}
