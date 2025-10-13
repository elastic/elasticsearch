/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.profile;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CustomTokenAuthenticator;
import org.elasticsearch.xpack.security.action.TransportGrantAction;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.PluggableAuthenticatorChain;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.profile.ProfileService;

import java.util.List;

public class TransportActivateProfileAction extends TransportGrantAction<ActivateProfileRequest, ActivateProfileResponse> {

    private final ProfileService profileService;
    private final List<CustomTokenAuthenticator> customTokenAuthenticators;

    @Inject
    public TransportActivateProfileAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ProfileService profileService,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ThreadPool threadPool,
        PluggableAuthenticatorChain pluggableAuthenticatorChain
    ) {
        super(
            ActivateProfileAction.NAME,
            transportService,
            actionFilters,
            authenticationService,
            authorizationService,
            threadPool.getThreadContext()
        );
        this.profileService = profileService;
        this.customTokenAuthenticators = pluggableAuthenticatorChain.getCustomAuthenticators()
            .stream()
            .filter(CustomTokenAuthenticator.class::isInstance)
            .map(CustomTokenAuthenticator.class::cast)
            .toList();
    }

    @Override
    protected void doExecuteWithGrantAuthentication(
        Task task,
        ActivateProfileRequest request,
        Authentication authentication,
        ActionListener<ActivateProfileResponse> listener
    ) {
        profileService.activateProfile(authentication, listener.map(ActivateProfileResponse::new));
    }

    @Override
    protected AuthenticationToken extractAccessToken(Grant grant) {
        for (CustomTokenAuthenticator customTokenAuthenticator : customTokenAuthenticators) {
            AuthenticationToken token = customTokenAuthenticator.extractGrantAccessToken(grant);
            if (token != null) {
                return token;
            }
        }
        return super.extractAccessToken(grant);
    }
}
