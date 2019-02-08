/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class GetCcrRestoreFileChunkAction extends Action<GetCcrRestoreFileChunkRequest,
    GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse, GetCcrRestoreFileChunkRequestBuilder> {

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

    @Override
    public GetCcrRestoreFileChunkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetCcrRestoreFileChunkRequestBuilder(client);
    }


    public static class TransportGetCcrRestoreFileChunkAction
        extends HandledTransportAction<GetCcrRestoreFileChunkRequest, GetCcrRestoreFileChunkResponse> {

        private final CcrRestoreSourceService restoreSourceService;
        private final BigArrays bigArrays;

        @Inject
        public TransportGetCcrRestoreFileChunkAction(Settings settings, BigArrays bigArrays, TransportService transportService,
                                                     IndexNameExpressionResolver resolver,
                                                     ActionFilters actionFilters, CcrRestoreSourceService restoreSourceService) {
            super(settings, NAME, transportService.getThreadPool(), transportService, actionFilters, resolver,
                GetCcrRestoreFileChunkRequest::new, ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, GetCcrRestoreFileChunkResponse::new);
            this.restoreSourceService = restoreSourceService;
            this.bigArrays = bigArrays;
        }

        @Override
        protected void doExecute(GetCcrRestoreFileChunkRequest request, ActionListener<GetCcrRestoreFileChunkResponse> listener) {
            int bytesRequested = request.getSize();
            ByteArray array = bigArrays.newByteArray(bytesRequested, false);
            String fileName = request.getFileName();
            String sessionUUID = request.getSessionUUID();
            // This is currently safe to do because calling `onResponse` will serialize the bytes to the network layer data
            // structure on the same thread. So the bytes will be copied before the reference is released.
            try (ReleasablePagedBytesReference reference = new ReleasablePagedBytesReference(array, bytesRequested, array)) {
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
        private final BytesReference chunk;

        GetCcrRestoreFileChunkResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
            offset = streamInput.readVLong();
            chunk = streamInput.readBytesReference();
        }

        GetCcrRestoreFileChunkResponse(long offset, BytesReference chunk) {
            this.offset = offset;
            this.chunk = chunk;
        }

        public long getOffset() {
            return offset;
        }

        public BytesReference getChunk() {
            return chunk;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(offset);
            out.writeBytesReference(chunk);
        }

    }
}
