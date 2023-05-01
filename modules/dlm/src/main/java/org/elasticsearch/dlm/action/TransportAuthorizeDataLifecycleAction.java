/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.dlm.AuthorizeDataLifecycleAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportAuthorizeDataLifecycleAction extends HandledTransportAction<
    AuthorizeDataLifecycleAction.Request,
    AcknowledgedResponse> {

    @Inject
    public TransportAuthorizeDataLifecycleAction(TransportService transportService, ActionFilters actionFilters) {
        super(AuthorizeDataLifecycleAction.NAME, transportService, actionFilters, AuthorizeDataLifecycleAction.Request::new);
    }

    @Override
    protected void doExecute(Task task, AuthorizeDataLifecycleAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // No-op, we just need to trigger authz
        listener.onResponse(AcknowledgedResponse.TRUE);
    }
}
