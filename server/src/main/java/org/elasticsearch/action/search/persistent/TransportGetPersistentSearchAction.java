/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.PersistentSearchService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.persistent.PersistentSearchResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportGetPersistentSearchAction extends HandledTransportAction<GetPersistentSearchRequest, PersistentSearchResponse> {

    private final PersistentSearchService persistentSearchService;

    @Inject
    public TransportGetPersistentSearchAction(TransportService transportService,
                                              ActionFilters actionFilters,
                                              PersistentSearchService persistentSearchService) {
        super(GetPersistentSearchAction.NAME, transportService, actionFilters, GetPersistentSearchRequest::new);
        this.persistentSearchService = persistentSearchService;
    }

    @Override
    protected void doExecute(Task task, GetPersistentSearchRequest request, ActionListener<PersistentSearchResponse> listener) {
        persistentSearchService.getPersistentSearchResponse(request, listener);
    }
}
