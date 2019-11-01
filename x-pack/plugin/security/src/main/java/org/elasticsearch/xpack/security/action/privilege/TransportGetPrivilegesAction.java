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
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesResponse;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.Strings.isNullOrEmpty;

/**
 * Transport action to retrieve one or more application privileges from the security index
 */
public class TransportGetPrivilegesAction extends HandledTransportAction<GetPrivilegesRequest, GetPrivilegesResponse> {

    private final NativePrivilegeStore privilegeStore;

    @Inject
    public TransportGetPrivilegesAction(ActionFilters actionFilters, NativePrivilegeStore privilegeStore,
                                        TransportService transportService) {
        super(GetPrivilegesAction.NAME, transportService, actionFilters, GetPrivilegesRequest::new);
        this.privilegeStore = privilegeStore;
    }

    @Override
    protected void doExecute(Task task, final GetPrivilegesRequest request, final ActionListener<GetPrivilegesResponse> listener) {
        final Set<String> names;
        if (request.privileges() == null || request.privileges().length == 0) {
            names = null;
        } else {
            names = new HashSet<>(Arrays.asList(request.privileges()));
        }

        final Collection<String> applications = isNullOrEmpty(request.application()) ? null : Collections.singleton(request.application());
        this.privilegeStore.getPrivileges(applications, names, ActionListener.wrap(
            privileges -> listener.onResponse(new GetPrivilegesResponse(privileges)),
            listener::onFailure
        ));
    }

}
