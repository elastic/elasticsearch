/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction.Response;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;


public class TransportGetDataFrameTransformsAction extends HandledTransportAction<Request, Response> {

    private final DataFrameTransformsConfigManager transformsConfigManager;

    @Inject
    public TransportGetDataFrameTransformsAction(TransportService transportService, ActionFilters actionFilters,
                                                 DataFrameTransformsConfigManager transformsConfigManager) {
        super(GetDataFrameTransformsAction.NAME, transportService, actionFilters, () -> new Request());
        this.transformsConfigManager = transformsConfigManager;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        //TODO support comma delimited and simple regex IDs
        transformsConfigManager.getTransformConfigurations(request.getId(), ActionListener.wrap(
            configs -> listener.onResponse(new Response(configs)),
            listener::onFailure
        ));
    }
}
