/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.migration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportRollupMigrationAction extends HandledTransportAction<RollupMigrationRequest, BulkByScrollResponse> {
    protected TransportRollupMigrationAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<RollupMigrationRequest> rollupMigrationRequestReader
    ) {
        super(actionName, transportService, actionFilters, rollupMigrationRequestReader);
    }

    @Override
    protected void doExecute(Task task, RollupMigrationRequest request, ActionListener<BulkByScrollResponse> listener) {
        // Delegate to reindex here?  Do we just create a TransportReindexAction?  Aren't we in a different module?
        ReindexRequest reindexRequest = new ReindexRequest(request.getSearchRequest(), request.getDestination());
        // Apply rollup metadata
    }
}
