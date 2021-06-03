/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.privilege;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesResponse;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Collections;
import java.util.Set;

/**
 * Transport action to retrieve one or more application privileges from the security index
 */
public class TransportDeletePrivilegesAction extends HandledTransportAction<DeletePrivilegesRequest, DeletePrivilegesResponse> {

    private final NativePrivilegeStore privilegeStore;

    @Inject
    public TransportDeletePrivilegesAction(ActionFilters actionFilters, NativePrivilegeStore privilegeStore,
                                           TransportService transportService) {
        super(DeletePrivilegesAction.NAME, transportService, actionFilters, DeletePrivilegesRequest::new);
        this.privilegeStore = privilegeStore;
    }

    @Override
    protected void doExecute(Task task, final DeletePrivilegesRequest request, final ActionListener<DeletePrivilegesResponse> listener) {
        if (request.privileges() == null || request.privileges().length == 0) {
            listener.onResponse(new DeletePrivilegesResponse(Collections.emptyList()));
            return;
        }
        final Set<String> names = Sets.newHashSet(request.privileges());
        this.privilegeStore.deletePrivileges(request.application(), names, request.getRefreshPolicy(), ActionListener.wrap(
                privileges -> listener.onResponse(
                        new DeletePrivilegesResponse(privileges.getOrDefault(request.application(), Collections.emptyList()))
                ), listener::onFailure
        ));
    }
}
