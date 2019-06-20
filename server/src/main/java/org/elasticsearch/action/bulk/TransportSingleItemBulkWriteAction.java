/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
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
                final DocWriteResponse response = bulkItemResponse.getResponse();
                listener.onResponse((Response) response);
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
