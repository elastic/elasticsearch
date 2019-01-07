/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class GetCcrRestoreFileChunkAction extends Action<GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse> {

    public static final GetCcrRestoreFileChunkAction INSTANCE = new GetCcrRestoreFileChunkAction();
    public static final String NAME = "internal:admin/ccr/restore/file_chunk/get";

    private GetCcrRestoreFileChunkAction() {
        super(NAME);
    }

    @Override
    public GetCcrRestoreFileChunkResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<GetCcrRestoreFileChunkResponse> getResponseReader() {
        return GetCcrRestoreFileChunkResponse::new;
    }


    public static class TransportGetCcrRestoreFileChunkAction
        extends HandledTransportAction<GetCcrRestoreFileChunkRequest, GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse> {

        private final CcrRestoreSourceService restoreSourceService;
        private final ThreadPool threadPool;
        private final BigArrays bigArrays;

        @Inject
        public TransportGetCcrRestoreFileChunkAction(BigArrays bigArrays, TransportService transportService, ActionFilters actionFilters,
                                                     CcrRestoreSourceService restoreSourceService) {
            super(NAME, transportService, actionFilters, GetCcrRestoreFileChunkRequest::new);
            TransportActionProxy.registerProxyAction(transportService, NAME, GetCcrRestoreFileChunkResponse::new);
            this.threadPool = transportService.getThreadPool();
            this.restoreSourceService = restoreSourceService;
            this.bigArrays = bigArrays;
        }

        @Override
        protected void doExecute(Task task, GetCcrRestoreFileChunkRequest request,
                                 ActionListener<GetCcrRestoreFileChunkResponse> listener) {
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() throws Exception {
                    int bytesRequested = request.getSize();
                    long fileOffset = request.getOffset();
                    ByteArray array = bigArrays.newByteArray(bytesRequested, false);
                    String fileName = request.getFileName();
                    String sessionUUID = request.getSessionUUID();
                    // This is currently safe to do because calling `onResponse` will serialize the bytes to the network layer data
                    // structure on the same thread. So the bytes will be copied before the reference is released.
                    try (ReleasablePagedBytesReference reference = new ReleasablePagedBytesReference(array, bytesRequested, array)) {
                        try (CcrRestoreSourceService.Reader reader = restoreSourceService.getSessionReader(sessionUUID, fileName)) {
                            BytesRefIterator refIterator = reference.iterator();
                            BytesRef ref;
                            int bytesWritten = 0;
                            while ((ref = refIterator.next()) != null) {
                                byte[] refBytes = ref.bytes;
                                reader.readFileBytes(refBytes, fileOffset + bytesWritten, ref.length);
                                bytesWritten += ref.length;
                            }

                            listener.onResponse(new GetCcrRestoreFileChunkResponse(reference));
                        }
                    }
                }
            });
        }
    }

    public static class GetCcrRestoreFileChunkResponse extends ActionResponse {

        private final BytesReference chunk;

        GetCcrRestoreFileChunkResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
            chunk = streamInput.readBytesReference();
        }

        GetCcrRestoreFileChunkResponse(BytesReference chunk) {
            this.chunk = chunk;
        }

        public BytesReference getChunk() {
            return chunk;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBytesReference(chunk);
        }
    }
}
