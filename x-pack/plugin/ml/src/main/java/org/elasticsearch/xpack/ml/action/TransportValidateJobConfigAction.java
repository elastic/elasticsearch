/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;

import java.util.function.Supplier;

public class TransportValidateJobConfigAction extends HandledTransportAction<ValidateJobConfigAction.Request,
        ValidateJobConfigAction.Response> {

    @Inject
    public TransportValidateJobConfigAction(Settings settings, TransportService transportService, ActionFilters actionFilters) {
        super(settings, ValidateJobConfigAction.NAME, transportService, actionFilters,
            (Supplier< ValidateJobConfigAction.Request>) ValidateJobConfigAction.Request::new);
    }

    @Override
    protected void doExecute(ValidateJobConfigAction.Request request, ActionListener<ValidateJobConfigAction.Response> listener) {
        listener.onResponse(new ValidateJobConfigAction.Response(true));
    }

}
