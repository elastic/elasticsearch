/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.engine.Engine;
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

        @Inject
        public TransportGetCcrRestoreFileChunkAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                                     CcrRestoreSourceService restoreSourceService) {
            super(NAME, transportService, actionFilters, GetCcrRestoreFileChunkRequest::new);
            TransportActionProxy.registerProxyAction(transportService, NAME, GetCcrRestoreFileChunkResponse::new);
            this.threadPool = threadPool;
            this.restoreSourceService = restoreSourceService;
        }

        @Override
        protected void doExecute(Task task, GetCcrRestoreFileChunkRequest request, ActionListener<GetCcrRestoreFileChunkResponse> listener) {
            threadPool.generic().execute(() -> {
                Engine.IndexCommitRef snapshot = restoreSourceService.getSession(request.getSessionUUID());
                try (IndexInput in = snapshot.getIndexCommit().getDirectory().openInput(request.getFileName(), IOContext.READONCE)) {
                    byte[] chunk = new byte[request.getSize()];
                    in.seek(request.getOffset());
                    in.readBytes(chunk, 0, request.getSize());
                    listener.onResponse(new GetCcrRestoreFileChunkResponse(new BytesArray(chunk)));
                } catch (IOException e) {
                    throw new ElasticsearchException(e);
                }
            });
        }
    }

    public static class GetCcrRestoreFileChunkResponse extends ActionResponse {

        private BytesReference chunk;

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
