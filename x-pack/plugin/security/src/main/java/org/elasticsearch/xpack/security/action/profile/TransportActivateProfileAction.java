/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.profile;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileResponse;
import org.elasticsearch.xpack.security.action.TransportGrantAction;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.profile.ProfileService;

public class TransportActivateProfileAction extends TransportGrantAction<ActivateProfileRequest, ActivateProfileResponse> {

    private final ProfileService profileService;

    @Inject
    public TransportActivateProfileAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ProfileService profileService,
        AuthenticationService authenticationService,
        ThreadPool threadPool
    ) {
        super(
            ActivateProfileAction.NAME,
            transportService,
            actionFilters,
            ActivateProfileRequest::new,
            authenticationService,
            threadPool.getThreadContext()
        );
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, ActivateProfileRequest request, ActionListener<ActivateProfileResponse> listener) {
        executeWithGrantAuthentication(
            request,
            listener.delegateFailure(
                (l, authentication) -> profileService.activateProfile(authentication, l.map(ActivateProfileResponse::new))
            )
        );
    }
}
