/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.search.persistent.PersistentSearchService;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class ReducePartialPersistentSearchAction extends ActionType<ReducePartialPersistentSearchResponse> {
    public static String NAME = "indices:data/read/persistent_search[phase/reduce]";
    public static ReducePartialPersistentSearchAction INSTANCE = new ReducePartialPersistentSearchAction();

    public ReducePartialPersistentSearchAction() {
        super(NAME, ReducePartialPersistentSearchResponse::new);
    }

    public static class TransportAction extends HandledTransportAction<ReducePartialPersistentSearchRequest,
                                                                                                    ReducePartialPersistentSearchResponse> {

        private final PersistentSearchService persistentSearchService;

        @Inject
        public TransportAction(TransportService transportService,
                                                            ActionFilters actionFilters,
                                                            PersistentSearchService persistentSearchService) {
            super(ReducePartialPersistentSearchAction.NAME, transportService, actionFilters, ReducePartialPersistentSearchRequest::new);
            this.persistentSearchService = persistentSearchService;
        }

        @Override
        protected void doExecute(Task task,
                                 ReducePartialPersistentSearchRequest request,
                                 ActionListener<ReducePartialPersistentSearchResponse> listener) {
            persistentSearchService.executePartialReduce(request, (SearchTask) task, listener);
        }
    }
}
