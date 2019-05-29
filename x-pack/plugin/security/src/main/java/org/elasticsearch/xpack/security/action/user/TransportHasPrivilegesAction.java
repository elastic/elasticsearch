/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action that tests whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
public class TransportHasPrivilegesAction extends HandledTransportAction<HasPrivilegesRequest, HasPrivilegesResponse> {

    private final AuthorizationService authorizationService;
    private final NativePrivilegeStore privilegeStore;

    @Inject
    public TransportHasPrivilegesAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        AuthorizationService authorizationService, NativePrivilegeStore privilegeStore) {
        super(settings, HasPrivilegesAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                HasPrivilegesRequest::new);
        this.authorizationService = authorizationService;
        this.privilegeStore = privilegeStore;
    }

    @Override
    protected void doExecute(HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener) {
        final String username = request.username();

        final Authentication authentication = Authentication.getAuthentication(threadPool.getThreadContext());
        final User user = authentication.getUser();
        if (user.principal().equals(username) == false) {
            listener.onFailure(new IllegalArgumentException("users may only check the privileges of their own account"));
            return;
        }

        resolveApplicationPrivileges(request, ActionListener.wrap(applicationPrivilegeDescriptors ->
                authorizationService.checkPrivileges(authentication, request, applicationPrivilegeDescriptors, listener),
            listener::onFailure));
    }

    private void resolveApplicationPrivileges(HasPrivilegesRequest request,
                                              ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener) {
        final Set<String> applications = getApplicationNames(request);
        privilegeStore.getPrivileges(applications, null, listener);
    }

    public static Set<String> getApplicationNames(HasPrivilegesRequest request) {
        return Arrays.stream(request.applicationPrivileges())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());
    }
}
