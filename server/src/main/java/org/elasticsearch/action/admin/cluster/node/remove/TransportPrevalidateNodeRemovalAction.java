/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.remove;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportPrevalidateNodeRemovalAction extends TransportAction<PrevalidateNodeRemovalRequest, PrevalidateNodeRemovalResponse> {

    @Inject
    public TransportPrevalidateNodeRemovalAction(ActionFilters actionFilters, TransportService transportService) {
        super(PrevalidateNodeRemovalAction.NAME, actionFilters, transportService.getTaskManager());
    }

    @Override
    protected void doExecute(Task task, PrevalidateNodeRemovalRequest request, ActionListener<PrevalidateNodeRemovalResponse> listener) {
        listener.onFailure(new RuntimeException("not implemented"));
    }
}
