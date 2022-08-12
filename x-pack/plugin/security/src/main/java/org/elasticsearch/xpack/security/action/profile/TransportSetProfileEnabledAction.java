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
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledAction;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledRequest;
import org.elasticsearch.xpack.security.profile.ProfileService;

public class TransportSetProfileEnabledAction extends HandledTransportAction<SetProfileEnabledRequest, AcknowledgedResponse> {

    private final ProfileService profileService;

    @Inject
    public TransportSetProfileEnabledAction(TransportService transportService, ActionFilters actionFilters, ProfileService profileService) {
        super(SetProfileEnabledAction.NAME, transportService, actionFilters, SetProfileEnabledRequest::new);
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, SetProfileEnabledRequest request, ActionListener<AcknowledgedResponse> listener) {
        profileService.setEnabled(request.getUid(), request.isEnabled(), request.getRefreshPolicy(), listener);
    }
}
