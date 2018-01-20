/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportValidateJobConfigAction extends HandledTransportAction<ValidateJobConfigAction.Request,
        ValidateJobConfigAction.Response> {

    @Inject
    public TransportValidateJobConfigAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ValidateJobConfigAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                ValidateJobConfigAction.Request::new);
    }

    @Override
    protected void doExecute(ValidateJobConfigAction.Request request, ActionListener<ValidateJobConfigAction.Response> listener) {
        listener.onResponse(new ValidateJobConfigAction.Response(true));
    }

}
