/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.delete;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the delete operation.
 *
 * Deprecated use TransportBulkAction with a single item instead
 */
@Deprecated
public class TransportDeleteAction extends TransportSingleItemBulkWriteAction<DeleteRequest, DeleteResponse> {

    public static final String NAME = "indices:data/write/delete";
    public static final ActionType<DeleteResponse> TYPE = new ActionType<>(NAME);

    @Inject
    public TransportDeleteAction(TransportService transportService, ActionFilters actionFilters, TransportBulkAction bulkAction) {
        super(NAME, transportService, actionFilters, DeleteRequest::new, bulkAction);
    }
}
