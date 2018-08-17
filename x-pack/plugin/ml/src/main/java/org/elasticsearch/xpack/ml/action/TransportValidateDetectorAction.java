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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;

public class TransportValidateDetectorAction extends HandledTransportAction<ValidateDetectorAction.Request,
        AcknowledgedResponse> {

    @Inject
    public TransportValidateDetectorAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ValidateDetectorAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                ValidateDetectorAction.Request::new);
    }

    @Override
    protected void doExecute(ValidateDetectorAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        listener.onResponse(new AcknowledgedResponse(true));
    }

}
