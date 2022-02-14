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
import org.elasticsearch.xpack.core.security.action.profile.SearchProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SearchProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SearchProfilesResponse;
import org.elasticsearch.xpack.security.profile.ProfileService;

public class TransportSearchProfilesAction extends HandledTransportAction<SearchProfilesRequest, SearchProfilesResponse> {

    private final ProfileService profileService;

    @Inject
    public TransportSearchProfilesAction(TransportService transportService, ActionFilters actionFilters, ProfileService profileService) {
        super(SearchProfilesAction.NAME, transportService, actionFilters, SearchProfilesRequest::new);
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, SearchProfilesRequest request, ActionListener<SearchProfilesResponse> listener) {
        profileService.searchProfile(request, listener);
    }
}
