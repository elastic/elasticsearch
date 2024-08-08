/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;

import static java.util.stream.Collectors.toList;

public final class TransportRemoteInfoAction extends HandledTransportAction<RemoteInfoRequest, RemoteInfoResponse> {

    public static final ActionType<RemoteInfoResponse> TYPE = new ActionType<>("cluster:monitor/remote/info");
    private final RemoteClusterService remoteClusterService;

    @Inject
    public TransportRemoteInfoAction(TransportService transportService, ActionFilters actionFilters) {
        super(TYPE.name(), transportService, actionFilters, RemoteInfoRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.remoteClusterService = transportService.getRemoteClusterService();
    }

    @Override
    protected void doExecute(Task task, RemoteInfoRequest remoteInfoRequest, ActionListener<RemoteInfoResponse> listener) {
        if (remoteClusterService.isEnabled() == false) {
            throw new IllegalArgumentException(
                "node ["
                    + remoteClusterService.getLocalNode().getName()
                    + "] does not have the ["
                    + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName()
                    + "] role"
            );
        }
        listener.onResponse(new RemoteInfoResponse(remoteClusterService.getRemoteConnectionInfos().collect(toList())));
    }
}
