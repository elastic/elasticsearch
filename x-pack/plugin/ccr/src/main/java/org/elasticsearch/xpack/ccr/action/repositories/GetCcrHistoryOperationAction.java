/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAwareRequest;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;
import java.util.List;

public class GetCcrHistoryOperationAction extends Action<GetCcrHistoryOperationAction.Response> {

    public static final GetCcrHistoryOperationAction INSTANCE = new GetCcrHistoryOperationAction();
    public static final String NAME = "internal:admin/ccr/restore/history_operations/get";

    private GetCcrHistoryOperationAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static class TransportGetCcrHistoryOperationAction extends HandledTransportAction<Request, Response> {
        private final CcrRestoreSourceService restoreSourceService;

        @Inject
        public TransportGetCcrHistoryOperationAction(TransportService transportService, ActionFilters actionFilters,
                                                     CcrRestoreSourceService restoreSourceService) {
            super(NAME, transportService, actionFilters, Request::new, ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, Response::new);
            this.restoreSourceService = restoreSourceService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            ActionListener.completeWith(listener, () -> {
                try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(request.sessionUUID)) {
                    final Tuple<List<Translog.Operation>, ByteSizeValue> batch = sessionReader.readHistoryOperations(
                        request.fromSeqNo, request.toSeqNo, request.maxBatchSize);
                    return new Response(batch.v1(), batch.v2());
                }
            });
        }
    }

    public static final class Request extends ActionRequest implements RemoteClusterAwareRequest {
        private final DiscoveryNode node;
        private final String sessionUUID;
        private final long fromSeqNo;
        private final long toSeqNo;
        private final ByteSizeValue maxBatchSize;

        public Request(DiscoveryNode node, String sessionUUID, long fromSeqNo, long toSeqNo, ByteSizeValue maxBatchSize) {
            this.node = node;
            this.sessionUUID = sessionUUID;
            this.fromSeqNo = fromSeqNo;
            this.toSeqNo = toSeqNo;
            this.maxBatchSize = maxBatchSize;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            node = null;
            sessionUUID = in.readString();
            fromSeqNo = in.readVLong();
            toSeqNo = in.readVLong();
            maxBatchSize = new ByteSizeValue(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public DiscoveryNode getPreferredTargetNode() {
            return node;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionUUID);
            out.writeVLong(fromSeqNo);
            out.writeVLong(toSeqNo);
            maxBatchSize.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException();
        }
    }

    public static final class Response extends ActionResponse {
        private final List<Translog.Operation> operations;
        private final ByteSizeValue batchSize;

        public Response(List<Translog.Operation> operations, ByteSizeValue batchSize) {
            this.operations = operations;
            this.batchSize = batchSize;
        }

        Response(StreamInput in) throws IOException {
            this.operations = Translog.readOperations(in, "remote_recovery");
            this.batchSize = new ByteSizeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Translog.writeOperations(out, operations);
            batchSize.writeTo(out);
        }

        public List<Translog.Operation> operations() {
            return operations;
        }

        public ByteSizeValue batchSize() {
            return batchSize;
        }
    }
}
