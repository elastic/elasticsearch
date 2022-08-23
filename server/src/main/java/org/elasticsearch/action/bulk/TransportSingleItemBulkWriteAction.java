/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * Use {@link TransportBulkAction} with {@link BulkRequest#fromSingleRequest(ReplicatedWriteRequest)}
 * and {@link TransportBulkAction#wrapBulkAsSingleItemResponse(ActionListener)} instead.
 */
@Deprecated
public abstract class TransportSingleItemBulkWriteAction<
    Request extends ReplicatedWriteRequest<Request>,
    Response extends ReplicationResponse & WriteResponse> extends HandledTransportAction<Request, Response> {

    private final TransportBulkAction bulkAction;

    protected TransportSingleItemBulkWriteAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        TransportBulkAction bulkAction
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.bulkAction = bulkAction;
    }

    @Override
    protected void doExecute(Task task, final Request request, final ActionListener<Response> listener) {
        bulkAction.execute(task, BulkRequest.fromSingleRequest(request), TransportBulkAction.wrapBulkAsSingleItemResponse(listener));
    }
}
