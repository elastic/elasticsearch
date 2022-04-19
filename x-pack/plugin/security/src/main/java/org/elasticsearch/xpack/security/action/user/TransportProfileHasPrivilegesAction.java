/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationContext;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportProfileHasPrivilegesAction extends HandledTransportAction<ProfileHasPrivilegesRequest, ProfileHasPrivilegesResponse> {

    private final AuthorizationService authorizationService;
    private final NativePrivilegeStore privilegeStore;

    private final SecurityContext securityContext;

    @Inject
    public TransportProfileHasPrivilegesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AuthorizationService authorizationService,
        NativePrivilegeStore privilegeStore,
        SecurityContext securityContext
    ) {
        super(ProfileHasPrivilegesAction.NAME, transportService, actionFilters, ProfileHasPrivilegesRequest::new);
        this.authorizationService = authorizationService;
        this.privilegeStore = privilegeStore;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(Task task, ProfileHasPrivilegesRequest request, ActionListener<ProfileHasPrivilegesResponse> listener) {
        final Subject subject = AuthenticationContext.fromAuthentication(securityContext.getAuthentication()).getEffectiveSubject();
        final AuthorizationEngine.AuthorizationInfo authorizationInfo = securityContext.getAuthorizationInfoFromContext();
        resolveApplicationPrivileges(
            request,
            ActionListener.wrap(
                applicationPrivilegeDescriptors -> listener.onResponse(new ProfileHasPrivilegesResponse()),
                listener::onFailure
            )
        );
    }

    private void resolveApplicationPrivileges(
        ProfileHasPrivilegesRequest request,
        ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
    ) {
        final Set<String> applications = Arrays.stream(request.applicationPrivileges())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());
        privilegeStore.getPrivileges(applications, null, listener);
    }
}
