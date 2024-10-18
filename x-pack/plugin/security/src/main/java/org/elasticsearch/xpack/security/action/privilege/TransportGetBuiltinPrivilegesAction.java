/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.privilege;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.TreeSet;

/**
 * Transport action to retrieve built-in (cluster/index) privileges
 */
public class TransportGetBuiltinPrivilegesAction extends TransportAction<GetBuiltinPrivilegesRequest, GetBuiltinPrivilegesResponse> {

    @Inject
    public TransportGetBuiltinPrivilegesAction(ActionFilters actionFilters, TransportService transportService) {
        super(GetBuiltinPrivilegesAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    @Override
    protected void doExecute(Task task, GetBuiltinPrivilegesRequest request, ActionListener<GetBuiltinPrivilegesResponse> listener) {
        final TreeSet<String> cluster = new TreeSet<>(ClusterPrivilegeResolver.names());
        final TreeSet<String> index = new TreeSet<>(IndexPrivilege.names());
        final TreeSet<String> remoteCluster = new TreeSet<>(RemoteClusterPermissions.getSupportedRemoteClusterPermissions());
        listener.onResponse(new GetBuiltinPrivilegesResponse(cluster, index, remoteCluster));
    }

}
