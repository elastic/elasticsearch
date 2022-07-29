/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the index operation.
 *
 * Allows for the following settings:
 * <ul>
 * <li><b>autoCreateIndex</b>: When set to {@code true}, will automatically create an index if one does not exists.
 * Defaults to {@code true}.
 * <li><b>allowIdGeneration</b>: If the id is set not, should it be generated. Defaults to {@code true}.
 * </ul>
 *
 * Deprecated use TransportBulkAction with a single item instead
 */
@Deprecated
public class TransportIndexAction extends TransportSingleItemBulkWriteAction<IndexRequest, IndexResponse> {

    @Inject
    public TransportIndexAction(ActionFilters actionFilters, TransportService transportService, TransportBulkAction bulkAction) {
        super(IndexAction.NAME, transportService, actionFilters, IndexRequest::new, bulkAction);
    }
}
