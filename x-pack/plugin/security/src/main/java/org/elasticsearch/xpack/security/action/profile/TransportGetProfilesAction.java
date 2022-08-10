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
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesResponse;
import org.elasticsearch.xpack.security.profile.ProfileService;

import java.util.List;

public class TransportGetProfilesAction extends HandledTransportAction<GetProfilesRequest, GetProfilesResponse> {

    private final ProfileService profileService;

    @Inject
    public TransportGetProfilesAction(TransportService transportService, ActionFilters actionFilters, ProfileService profileService) {
        super(GetProfilesAction.NAME, transportService, actionFilters, GetProfilesRequest::new);
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, GetProfilesRequest request, ActionListener<GetProfilesResponse> listener) {
        profileService.getProfiles(
            request.getUids(),
            request.getDataKeys(),
            listener.map(resultsAndError -> new GetProfilesResponse(List.copyOf(resultsAndError.results()), resultsAndError.errors()))
        );
    }
}
