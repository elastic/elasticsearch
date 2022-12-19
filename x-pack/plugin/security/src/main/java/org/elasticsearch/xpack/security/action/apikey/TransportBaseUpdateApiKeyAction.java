/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.BaseUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.support.ApiKeyUserRoleDescriptorResolver;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Set;

public abstract class TransportBaseUpdateApiKeyAction<Request extends BaseUpdateApiKeyRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    private final SecurityContext securityContext;
    private final ApiKeyUserRoleDescriptorResolver resolver;

    protected TransportBaseUpdateApiKeyAction(
        final String actionName,
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Writeable.Reader<Request> requestReader,
        final SecurityContext context,
        final CompositeRolesStore rolesStore,
        final NamedXContentRegistry xContentRegistry
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.securityContext = context;
        this.resolver = new ApiKeyUserRoleDescriptorResolver(rolesStore, xContentRegistry);
    }

    @Override
    public final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
            return;
        } else if (authentication.isApiKey()) {
            listener.onFailure(
                new IllegalArgumentException("authentication via API key not supported: only the owner user can update an API key")
            );
            return;
        }

        resolver.resolveUserRoleDescriptors(
            authentication,
            ActionListener.wrap(
                roleDescriptors -> doExecuteUpdate(task, request, authentication, roleDescriptors, listener),
                listener::onFailure
            )
        );
    }

    abstract void doExecuteUpdate(
        Task task,
        Request request,
        Authentication authentication,
        Set<RoleDescriptor> roleDescriptors,
        ActionListener<Response> listener
    );
}
