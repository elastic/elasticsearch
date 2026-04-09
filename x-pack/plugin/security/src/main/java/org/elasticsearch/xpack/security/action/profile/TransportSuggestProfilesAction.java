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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesResponse;
import org.elasticsearch.xpack.security.profile.ProfileService;

public class TransportSuggestProfilesAction extends HandledTransportAction<SuggestProfilesRequest, SuggestProfilesResponse> {

    private final ProfileService profileService;
    private final ClusterService clusterService;

    @Inject
    public TransportSuggestProfilesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ProfileService profileService,
        ClusterService clusterService
    ) {
        super(
            SuggestProfilesAction.NAME,
            transportService,
            actionFilters,
            SuggestProfilesRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.profileService = profileService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, SuggestProfilesRequest request, ActionListener<SuggestProfilesResponse> listener) {
        profileService.suggestProfile(request, new TaskId(clusterService.localNode().getId(), task.getId()), listener);
    }
}
