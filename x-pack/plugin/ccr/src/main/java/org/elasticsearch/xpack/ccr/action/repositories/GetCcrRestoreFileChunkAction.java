/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class GetCcrRestoreFileChunkAction extends ActionType<GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse> {

    public static final GetCcrRestoreFileChunkAction INSTANCE = new GetCcrRestoreFileChunkAction();
    public static final String NAME = "internal:admin/ccr/restore/file_chunk/get";

    private GetCcrRestoreFileChunkAction() {
        super(NAME, GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse::new);
    }

    public static class TransportGetCcrRestoreFileChunkAction extends HandledTransportAction<
        GetCcrRestoreFileChunkRequest,
        GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse> {

        private final CcrRestoreSourceService restoreSourceService;
        private final BigArrays bigArrays;

        @Inject
        public TransportGetCcrRestoreFileChunkAction(
            BigArrays bigArrays,
            TransportService transportService,
            ActionFilters actionFilters,
            CcrRestoreSourceService restoreSourceService
        ) {
            super(NAME, transportService, actionFilters, GetCcrRestoreFileChunkRequest::new, ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, false, GetCcrRestoreFileChunkResponse::new);
            this.restoreSourceService = restoreSourceService;
            this.bigArrays = bigArrays;
        }

        @Override
        protected void doExecute(
            Task task,
            GetCcrRestoreFileChunkRequest request,
            ActionListener<GetCcrRestoreFileChunkResponse> listener
        ) {
            int bytesRequested = request.getSize();
            ByteArray array = bigArrays.newByteArray(bytesRequested, false);
            String fileName = request.getFileName();
            String sessionUUID = request.getSessionUUID();
            BytesReference pagedBytesReference = BytesReference.fromByteArray(array, bytesRequested);
            try (ReleasableBytesReference reference = new ReleasableBytesReference(pagedBytesReference, array)) {
                try (CcrRestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(sessionUUID)) {
                    long offsetAfterRead = sessionReader.readFileBytes(fileName, reference);
                    long offsetBeforeRead = offsetAfterRead - reference.length();
                    listener.onResponse(new GetCcrRestoreFileChunkResponse(offsetBeforeRead, reference));
                }
            } catch (IOException e) {
                listener.onFailure(e);
            }
        }
    }

    public static class GetCcrRestoreFileChunkResponse extends ActionResponse {

        private final long offset;
        private final ReleasableBytesReference chunk;

        GetCcrRestoreFileChunkResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
            offset = streamInput.readVLong();
            chunk = streamInput.readReleasableBytesReference();
        }

        GetCcrRestoreFileChunkResponse(long offset, ReleasableBytesReference chunk) {
            this.offset = offset;
            this.chunk = chunk.retain();
        }

        public long getOffset() {
            return offset;
        }

        public ReleasableBytesReference getChunk() {
            return chunk;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(offset);
            out.writeBytesReference(chunk);
        }

        @Override
        public void incRef() {
            chunk.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return chunk.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return chunk.decRef();
        }

        @Override
        public boolean hasReferences() {
            return chunk.hasReferences();
        }
    }
}
