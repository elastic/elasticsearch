/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.privilege;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.TreeSet;

/**
 * Transport action to retrieve one or more application privileges from the security index
 */
public class TransportGetBuiltinPrivilegesAction extends HandledTransportAction<GetBuiltinPrivilegesRequest, GetBuiltinPrivilegesResponse> {

    @Inject
    public TransportGetBuiltinPrivilegesAction(ActionFilters actionFilters, TransportService transportService) {
        super(GetBuiltinPrivilegesAction.NAME, transportService, actionFilters, GetBuiltinPrivilegesRequest::new);
    }

    @Override
    protected void doExecute(Task task, GetBuiltinPrivilegesRequest request, ActionListener<GetBuiltinPrivilegesResponse> listener) {
        final TreeSet<String> cluster = new TreeSet<>(ClusterPrivilegeResolver.names());
        final TreeSet<String> index = new TreeSet<>(IndexPrivilege.names());
        listener.onResponse(new GetBuiltinPrivilegesResponse(cluster, index));
    }

}
