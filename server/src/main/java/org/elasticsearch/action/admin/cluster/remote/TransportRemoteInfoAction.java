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

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;

import static java.util.stream.Collectors.toList;

public final class TransportRemoteInfoAction extends HandledTransportAction<RemoteInfoRequest, RemoteInfoResponse> {

    private final RemoteClusterService remoteClusterService;

    @Inject
    public TransportRemoteInfoAction(TransportService transportService, ActionFilters actionFilters,
                                     SearchTransportService searchTransportService) {
        super(RemoteInfoAction.NAME, transportService, actionFilters, RemoteInfoRequest::new);
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
    }

    @Override
    protected void doExecute(Task task, RemoteInfoRequest remoteInfoRequest, ActionListener<RemoteInfoResponse> listener) {
        listener.onResponse(new RemoteInfoResponse(remoteClusterService.getRemoteConnectionInfos().collect(toList())));
    }
}
