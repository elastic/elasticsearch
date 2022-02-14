/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.profile;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesResponse;
import org.elasticsearch.xpack.security.profile.ProfileService;

public class TransportGetProfileAction extends HandledTransportAction<GetProfileRequest, GetProfilesResponse> {

    private final ProfileService profileService;

    @Inject
    public TransportGetProfileAction(TransportService transportService, ActionFilters actionFilters, ProfileService profileService) {
        super(GetProfileAction.NAME, transportService, actionFilters, GetProfileRequest::new);
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, GetProfileRequest request, ActionListener<GetProfilesResponse> listener) {
        profileService.getProfile(request.getUid(), request.getDataKeys(), listener.map(GetProfilesResponse::new));
    }
}
