/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.profile;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.BearerToken;
import org.elasticsearch.xpack.security.profile.ProfileService;

public class TransportActivateProfileAction extends HandledTransportAction<ActivateProfileRequest, ActivateProfileResponse> {

    private final ProfileService profileService;
    private final AuthenticationService authenticationService;
    private final ThreadContext threadContext;

    @Inject
    public TransportActivateProfileAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ProfileService profileService,
        AuthenticationService authenticationService,
        ThreadPool threadPool
    ) {
        super(ActivateProfileAction.NAME, transportService, actionFilters, ActivateProfileRequest::new);
        this.profileService = profileService;
        this.authenticationService = authenticationService;
        this.threadContext = threadPool.getThreadContext();
    }

    @Override
    protected void doExecute(Task task, ActivateProfileRequest request, ActionListener<ActivateProfileResponse> listener) {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            resolveAuthentication(
                request.getGrant(),
                request,
                ActionListener.wrap(
                    authentication -> profileService.activateProfile(authentication, listener.map(ActivateProfileResponse::new)),
                    listener::onFailure
                )
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // TODO: extract
    private void resolveAuthentication(
        GrantApiKeyRequest.Grant grant,
        TransportRequest transportRequest,
        ActionListener<Authentication> listener
    ) {
        switch (grant.getType()) {
            case GrantApiKeyRequest.PASSWORD_GRANT_TYPE:
                final UsernamePasswordToken token = new UsernamePasswordToken(grant.getUsername(), grant.getPassword());
                authenticationService.authenticate(super.actionName, transportRequest, token, listener);
                return;
            case GrantApiKeyRequest.ACCESS_TOKEN_GRANT_TYPE:
                authenticationService.authenticate(super.actionName, transportRequest, new BearerToken(grant.getAccessToken()), listener);
                return;
            default:
                listener.onFailure(new ElasticsearchSecurityException("the grant type [{}] is not supported", grant.getType()));
                return;
        }
    }
}
