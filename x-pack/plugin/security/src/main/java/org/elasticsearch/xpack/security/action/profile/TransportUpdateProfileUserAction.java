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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileUserAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileUserRequest;
import org.elasticsearch.xpack.security.profile.ProfileService;

public class TransportUpdateProfileUserAction extends HandledTransportAction<UpdateProfileUserRequest, AcknowledgedResponse> {

    private final ProfileService profileService;

    @Inject
    public TransportUpdateProfileUserAction(TransportService transportService, ActionFilters actionFilters, ProfileService profileService) {
        super(UpdateProfileUserAction.NAME, transportService, actionFilters, UpdateProfileUserRequest::new);
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, UpdateProfileUserRequest request, ActionListener<AcknowledgedResponse> listener) {
        profileService.updateProfileUser(request, listener);
    }
}
