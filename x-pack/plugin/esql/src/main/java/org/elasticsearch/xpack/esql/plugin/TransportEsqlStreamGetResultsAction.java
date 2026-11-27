/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlStreamGetResultAction;
import org.elasticsearch.xpack.esql.action.EsqlStreamQueryAction.ResultStream;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.StoredPage;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.StreamMetadata;
import org.elasticsearch.xpack.esql.streaming.StreamMetadataDocument;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;
import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.INDEX;

/**
 * Reads a stored stream result on the node that receives the request. The request carries a stream id and no indices, so this action
 * must check that the caller started the stream before reading it. That check, and the {@code RBACEngine} wiring that lets the action
 * through on a secured cluster, come with the stream storage.
 */
public class TransportEsqlStreamGetResultsAction extends TransportAction<GetStreamResultRequest, ActionResponse.Empty> {

    private final Client client;
    private final ProjectResolver projectResolver;
    private final BlockFactory blockFactory;

    @Inject
    public TransportEsqlStreamGetResultsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ProjectResolver projectResolver,
        BlockFactoryProvider blockFactoryProvider
    ) {
        super(EsqlStreamGetResultAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.projectResolver = projectResolver;
        this.blockFactory = blockFactoryProvider.blockFactory();
    }

    @Override
    protected void doExecute(Task task, GetStreamResultRequest request, ActionListener<ActionResponse.Empty> listener) {
        Client storeClient = new OriginSettingClient(client.projectClient(projectResolver.getProjectId()), ESQL_ORIGIN);
        fetchStream(storeClient, request.streamId(), listener.delegateFailureAndWrap((l, stream) -> {
            if (request.resultStreamListener() == null) {
                throw new IllegalStateException("[resultStreamListener] is required to read a stream");
            }
            new PageProducer((CancellableTask) task, request, stream, storeClient, l).start();
        }));
    }

    private static void fetchStream(Client storeClient, String streamId, ActionListener<StoredStream> listener) {
        storeClient.prepareGet(INDEX, streamId).setRouting(streamId).execute(ActionListener.wrap(get -> {
            if (get.isExists() == false) {
                throw new ResourceNotFoundException(streamId);
            }
            listener.onResponse(storedStream(StreamMetadataDocument.fromSource(get.getSourceAsMap())));
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onFailure(new ResourceNotFoundException(streamId));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private static TimeValue waitTimeout(GetStreamResultRequest request) {
        long millis = request.waitTimeout().millis();
        if (millis <= 0) {
            return TimeValue.ZERO;
        }
        return TimeValue.timeValueMillis(Math.min(millis, TransportReadStreamPagesAction.MAX_WAIT_TIMEOUT.millis()));
    }

    private final class PageProducer {
        private final CancellableTask task;
        private final GetStreamResultRequest request;
        private final StoredStream stream;
        private final Client storeClient;
        private final ActionListener<ActionResponse.Empty> listener;
        private final PageStreamPublisher publisher = new PageStreamPublisher();
        private final PageStreamPublisher.Producer producer = publisher.registerProducer();
        private volatile boolean complete;
        private boolean firstRead = true;
        private int nextSeq;
        private long endRow;

        PageProducer(
            CancellableTask task,
            GetStreamResultRequest request,
            StoredStream stream,
            Client storeClient,
            ActionListener<ActionResponse.Empty> listener
        ) {
            this.task = task;
            this.request = request;
            this.stream = stream;
            this.storeClient = storeClient;
            this.listener = listener;
            this.endRow = request.offset();
        }

        void start() {
            if (task.notifyIfCancelled(listener)) {
                return;
            }
            boolean partial = stream.footer().isPartial();
            request.resultStreamListener()
                .onResponse(
                    new ResultStream(
                        stream.columns(),
                        publisher,
                        null,
                        ZoneOffset.UTC,
                        null,
                        () -> partial,
                        request.offset(),
                        () -> complete
                    )
                );
            if (publisher.failure() != null) {
                end();
            } else if (request.statusOnly()) {
                finish(stream);
            } else {
                readWhenRequested();
            }
        }

        private void readWhenRequested() {
            publisher.waitForWriting().listener().addListener(ActionListener.wrap(ignored -> {
                if (publisher.isClosed()) {
                    end();
                } else if (task.isCancelled()) {
                    fail(task.getTaskCancelledException());
                } else {
                    readPage();
                }
            }, this::fail));
        }

        private void readPage() {
            TimeValue waitTimeout = waitTimeout(request);
            TransportReadStreamPagesAction.Request read = firstRead
                ? TransportReadStreamPagesAction.Request.fromRow(request.streamId(), request.offset(), waitTimeout)
                : new TransportReadStreamPagesAction.Request(request.streamId(), nextSeq, waitTimeout);
            storeClient.execute(TransportReadStreamPagesAction.TYPE, read, ActionListener.wrap(response -> {
                StoredPage stored = response.page();
                if (stored == null) {
                    finishWithLatestStream();
                    return;
                }
                Page page = decode(stored, request.offset());
                firstRead = false;
                nextSeq = stored.seq() + 1;
                endRow = stored.firstRow() + stored.rowCount();
                if (producer.addPage(page) == false) {
                    end();
                } else if (response.resultEnded()) {
                    finishWithLatestStream();
                } else {
                    readWhenRequested();
                }
            }, this::fail));
        }

        private void finishWithLatestStream() {
            fetchStream(storeClient, request.streamId(), ActionListener.wrap(this::finish, this::fail));
        }

        private void finish(StoredStream latest) {
            complete = latest.completeAt(endRow);
            producer.finish();
            publisher.completeWithFooter(latest.footer());
            end();
        }

        private void fail(Exception e) {
            publisher.failStream(e);
            listener.onFailure(e);
        }

        private void end() {
            Exception failure = publisher.failure();
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            }
        }
    }

    private Page decode(StoredPage stored, long offset) throws IOException {
        try (BlockStreamInput in = new BlockStreamInput(stored.bytes().streamInput(), blockFactory)) {
            in.setTransportVersion(stored.transportVersion());
            Page page = new Page(in);
            int skip = Math.toIntExact(Math.max(0L, offset - stored.firstRow()));
            if (skip == 0) {
                return page;
            }
            try {
                return page.slice(skip, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private static StoredStream storedStream(StreamMetadata metadata) {
        return new StoredStream(metadata, metadata.columns().stream().map(ColumnInfoImpl.class::cast).toList(), footer(metadata));
    }

    private static PageStreamPublisher.StreamFooter footer(StreamMetadata metadata) {
        StreamMetadata.StreamCompletionInfo completion = metadata.completionInfo();
        long tookMillis = completion == null ? 0L : completion.tookMillis();
        boolean partial = completion != null && completion.partial();
        return switch (metadata.status()) {
            case RUNNING, COMPLETE -> new PageStreamPublisher.StreamFooter(
                RestStatus.OK.getStatus(),
                tookMillis,
                partial,
                metadata.warnings(),
                null,
                null,
                null,
                null
            );
            case FAILED -> failureFooter(
                tookMillis,
                partial,
                metadata,
                new ElasticsearchStatusException("{}", completion.failure().status(), completion.failure().reason())
            );
            case CANCELED -> failureFooter(tookMillis, partial, metadata, new TaskCancelledException("the stream was canceled"));
        };
    }

    private static PageStreamPublisher.StreamFooter failureFooter(
        long tookMillis,
        boolean partial,
        StreamMetadata metadata,
        Exception error
    ) {
        return new PageStreamPublisher.StreamFooter(
            ExceptionsHelper.status(error).getStatus(),
            tookMillis,
            partial,
            metadata.warnings(),
            null,
            null,
            error,
            null
        );
    }

    private record StoredStream(StreamMetadata metadata, List<ColumnInfoImpl> columns, PageStreamPublisher.StreamFooter footer) {

        boolean completeAt(long endRow) {
            StreamMetadata.StreamCompletionInfo completion = metadata.completionInfo();
            return metadata.status() == StreamMetadata.Status.COMPLETE && completion.rows() != null && endRow >= completion.rows();
        }
    }
}
