/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;

public class TransportValidateJobConfigAction extends HandledTransportAction<ValidateJobConfigAction.Request, AcknowledgedResponse> {

    @Inject
    public TransportValidateJobConfigAction(TransportService transportService, ActionFilters actionFilters) {
        super(
            ValidateJobConfigAction.NAME,
            transportService,
            actionFilters,
            ValidateJobConfigAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void doExecute(Task task, ValidateJobConfigAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        listener.onResponse(AcknowledgedResponse.TRUE);
    }

}
