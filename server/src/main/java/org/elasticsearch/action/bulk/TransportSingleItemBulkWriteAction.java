/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/** use transport bulk action directly */
@Deprecated
public abstract class TransportSingleItemBulkWriteAction<
    Request extends ReplicatedWriteRequest<Request>,
    Response extends ReplicationResponse & WriteResponse
    > extends HandledTransportAction<Request, Response> {

    private final TransportBulkAction bulkAction;

    protected TransportSingleItemBulkWriteAction(String actionName, TransportService transportService, ActionFilters actionFilters,
                                                 Writeable.Reader<Request> requestReader, TransportBulkAction bulkAction) {
        super(actionName, transportService, actionFilters, requestReader);
        this.bulkAction = bulkAction;
    }

    @Override
    protected void doExecute(Task task, final Request request, final ActionListener<Response> listener) {
        bulkAction.execute(task, toSingleItemBulkRequest(request), wrapBulkResponse(listener));
    }

    public static <Response extends ReplicationResponse & WriteResponse>
    ActionListener<BulkResponse> wrapBulkResponse(ActionListener<Response> listener) {
        return ActionListener.wrap(bulkItemResponses -> {
            assert bulkItemResponses.getItems().length == 1 : "expected only one item in bulk request";
            BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
            if (bulkItemResponse.isFailed() == false) {
                @SuppressWarnings("unchecked")
                final Response response = (Response) bulkItemResponse.getResponse();
                listener.onResponse(response);
            } else {
                listener.onFailure(bulkItemResponse.getFailure().getCause());
            }
        }, listener::onFailure);
    }

    public static BulkRequest toSingleItemBulkRequest(ReplicatedWriteRequest<?> request) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(((DocWriteRequest<?>) request));
        bulkRequest.setRefreshPolicy(request.getRefreshPolicy());
        bulkRequest.timeout(request.timeout());
        bulkRequest.waitForActiveShards(request.waitForActiveShards());
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        return bulkRequest;
    }
}
