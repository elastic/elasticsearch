/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesResponse;

public class TransportProfileHasPrivilegesAction extends HandledTransportAction<ProfileHasPrivilegesRequest, ProfileHasPrivilegesResponse> {

    protected TransportProfileHasPrivilegesAction(TransportService transportService, ActionFilters actionFilters) {
        super(ProfileHasPrivilegesAction.NAME, transportService, actionFilters, ProfileHasPrivilegesRequest::new);
    }

    @Override
    protected void doExecute(Task task, ProfileHasPrivilegesRequest request, ActionListener<ProfileHasPrivilegesResponse> listener) {
        listener.onResponse(new ProfileHasPrivilegesResponse());
    }
}
