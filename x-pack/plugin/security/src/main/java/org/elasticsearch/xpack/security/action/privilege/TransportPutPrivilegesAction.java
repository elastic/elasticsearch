/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.privilege;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesResponse;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Transport action to retrieve one or more application privileges from the security index
 */
public class TransportPutPrivilegesAction extends HandledTransportAction<PutPrivilegesRequest, PutPrivilegesResponse> {

    private final NativePrivilegeStore privilegeStore;

    @Inject
    public TransportPutPrivilegesAction(
        ActionFilters actionFilters,
        NativePrivilegeStore privilegeStore,
        TransportService transportService
    ) {
        super(PutPrivilegesAction.NAME, transportService, actionFilters, PutPrivilegesRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.privilegeStore = privilegeStore;
    }

    @Override
    protected void doExecute(Task task, final PutPrivilegesRequest request, final ActionListener<PutPrivilegesResponse> listener) {
        if (request.getPrivileges() == null || request.getPrivileges().size() == 0) {
            listener.onResponse(new PutPrivilegesResponse(Collections.emptyMap()));
        } else {
            this.privilegeStore.putPrivileges(
                request.getPrivileges(),
                request.getRefreshPolicy(),
                ActionListener.wrap(result -> listener.onResponse(buildResponse(result)), listener::onFailure)
            );
        }
    }

    private static PutPrivilegesResponse buildResponse(Map<String, Map<String, DocWriteResponse.Result>> result) {
        final Map<String, List<String>> createdPrivilegesByApplicationName = Maps.newHashMapWithExpectedSize(result.size());
        result.forEach((appName, privileges) -> {
            List<String> createdPrivileges = privileges.entrySet()
                .stream()
                .filter(e -> e.getValue() == DocWriteResponse.Result.CREATED)
                .map(e -> e.getKey())
                .toList();
            if (createdPrivileges.isEmpty() == false) {
                createdPrivilegesByApplicationName.put(appName, createdPrivileges);
            }
        });
        return new PutPrivilegesResponse(createdPrivilegesByApplicationName);
    }
}
