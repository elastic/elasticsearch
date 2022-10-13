/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryRewriter;

public class XSearchSearchTransportAction extends HandledTransportAction<XSearchSearchAction.Request, XSearchSearchAction.Response> {

    private static final Logger LOGGER = LogManager.getLogger(XSearchSearchTransportAction.class);

    private final RelevanceMatchQueryRewriter relevanceMatchQueryRewriter;

    @Inject
    public XSearchSearchTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        String executor,
        RelevanceMatchQueryRewriter relevanceMatchQueryRewriter
    ) {
        super(XSearchSearchAction.NAME, false, transportService, actionFilters, XSearchSearchAction.Request::new, executor);
        this.relevanceMatchQueryRewriter = relevanceMatchQueryRewriter;
    }

    @Override
    protected void doExecute(Task task, XSearchSearchAction.Request request, ActionListener<XSearchSearchAction.Response> listener) {

        String relevanceSettingsId = "123";
        String curationsSettingsId = "456";

        RelevanceMatchQueryBuilder relevanceMatchQueryBuilder = new RelevanceMatchQueryBuilder(
            relevanceMatchQueryRewriter,
            request.getQuery(),
            relevanceSettingsId,
            curationsSettingsId
        );

        listener.onResponse(new XSearchSearchAction.Response(request.getQuery()));
        listener.onFailure(new UnsupportedOperationException("oh no!"));
    }
}
