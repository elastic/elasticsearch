/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * {@link ExchangeService} is responsible for exchanging pages between exchange sinks and sources on the same or different nodes.
 * It holds a map of {@link ExchangeSourceHandler} and {@link ExchangeSinkHandler} instances for each node in the cluster.
 * To connect exchange sources to exchange sinks, use the {@link ExchangeSourceHandler#addRemoteSink(RemoteSink, int)} method.
 * TODO:
 * - Add a reaper that removes/closes inactive sinks (i.e., no sink, source for more than 30 seconds)
 */
public final class ExchangeService {
    // TODO: Make this a child action of the data node transport to ensure that exchanges
    // are accessed only by the user initialized the session.
    public static final String EXCHANGE_ACTION_NAME = "internal:data/read/esql/exchange";
    private final TransportService transportService;

    private final Map<String, ExchangeSinkHandler> sinks = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PendingListener> pendingListeners = ConcurrentCollections.newConcurrentMap();
    private final Map<String, ExchangeSourceHandler> sources = ConcurrentCollections.newConcurrentMap();

    private final Executor fetchExecutor;

    public ExchangeService(TransportService transportService, ThreadPool threadPool) {
        this.transportService = transportService;
        this.fetchExecutor = threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
        transportService.registerRequestHandler(
            EXCHANGE_ACTION_NAME,
            ThreadPool.Names.SAME,
            ExchangeRequest::new,
            new ExchangeTransportAction()
        );
    }

    /**
     * Creates an {@link ExchangeSinkHandler} for the specified exchange id.
     *
     * @throws IllegalStateException if a sink handler for the given id already exists
     */
    public ExchangeSinkHandler createSinkHandler(String exchangeId, int maxBufferSize) {
        ExchangeSinkHandler sinkHandler = new ExchangeSinkHandler(maxBufferSize);
        if (sinks.putIfAbsent(exchangeId, sinkHandler) != null) {
            throw new IllegalStateException("sink exchanger for id [" + exchangeId + "] already exists");
        }
        final PendingListener pendingListener = pendingListeners.remove(exchangeId);
        if (pendingListener != null) {
            pendingListener.onReady(sinkHandler);
        }
        return sinkHandler;
    }

    /**
     * Returns an exchange sink handler for the given id.
     */
    public ExchangeSinkHandler getSinkHandler(String exchangeId, boolean failsIfNotExists) {
        ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
        if (sinkHandler == null && failsIfNotExists) {
            throw new IllegalStateException("sink exchanger for id [" + exchangeId + "] doesn't exist");
        }
        return sinkHandler;
    }

    /**
     * Creates an {@link ExchangeSourceHandler} for the specified exchange id.
     *
     * @throws IllegalStateException if a source handler for the given id already exists
     */
    public ExchangeSourceHandler createSourceHandler(String exchangeId, int maxBufferSize) {
        ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(maxBufferSize, fetchExecutor);
        if (sources.putIfAbsent(exchangeId, sourceHandler) != null) {
            throw new IllegalStateException("source exchanger for id [" + exchangeId + "] already exists");
        }
        return sourceHandler;
    }

    /**
     * Returns an exchange source handler for the given id.
     */
    public ExchangeSourceHandler getSourceHandler(String exchangeId, boolean failsIfNotExists) {
        ExchangeSourceHandler sourceHandler = sources.get(exchangeId);
        if (sourceHandler == null && failsIfNotExists) {
            throw new IllegalStateException("source exchanger for id [" + exchangeId + "] doesn't exist");
        }
        return sourceHandler;
    }

    /**
     * Mark an exchange sink handler for the given id as completed and remove it from the list.
     */
    public void completeSinkHandler(String exchangeId) {
        // TODO:
        // - Should make the sink as completed so subsequent exchange requests can be completed
        // - Remove the sinks map
        ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
        if (sinkHandler != null) {
            sinkHandler.finish();
        }
    }

    /**
     * Mark an exchange sink source for the given id as completed and remove it from the list.
     */
    public void completeSourceHandler(String exchangeId) {
        // TODO: Should abort outstanding exchange requests
        sources.remove(exchangeId);
    }

    private class ExchangeTransportAction implements TransportRequestHandler<ExchangeRequest> {
        @Override
        public void messageReceived(ExchangeRequest request, TransportChannel channel, Task task) {
            final String exchangeId = request.exchangeId();
            final ChannelActionListener<ExchangeResponse> listener = new ChannelActionListener<>(channel);
            ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
            if (sinkHandler != null) {
                sinkHandler.fetchPageAsync(request.sourcesFinished(), listener);
            } else if (request.sourcesFinished()) {
                listener.onResponse(new ExchangeResponse(null, true));
            } else {
                // If a data-node request arrives after an exchange request, we add the listener to the pending list. This allows the
                // data-node request to link the pending listeners with its exchange sink handler when it arrives. We also register the
                // listener to the task cancellation in case the data-node request never arrives due to a network issue or rejection.
                ActionListener<ExchangeResponse> wrappedListener = ActionListener.notifyOnce(listener);
                CancellableTask cancellableTask = (CancellableTask) task;
                cancellableTask.addListener(() -> cancellableTask.notifyIfCancelled(wrappedListener));
                pendingListeners.computeIfAbsent(exchangeId, k -> new PendingListener()).addListener(wrappedListener);
                // If the data-node request arrived while we were adding the listener to the pending list, we must complete the pending
                // listeners with the newly created sink handler.
                sinkHandler = sinks.get(exchangeId);
                if (sinkHandler != null) {
                    final PendingListener pendingListener = pendingListeners.remove(exchangeId);
                    if (pendingListener != null) {
                        pendingListener.onReady(sinkHandler);
                    }
                }
            }
        }
    }

    static final class PendingListener {
        private final List<ActionListener<ExchangeResponse>> listeners = Collections.synchronizedList(new ArrayList<>());

        void addListener(ActionListener<ExchangeResponse> listener) {
            listeners.add(listener);
        }

        void onReady(ExchangeSinkHandler handler) {
            for (var listener : listeners) {
                handler.fetchPageAsync(false, listener);
            }
        }
    }

    /**
     * Creates a new {@link RemoteSink} that fetches pages from an exchange sink located on the remote node.
     *
     * @param remoteNode the node where the remote exchange sink is located
     * @param parentTask the parent task that initialized the ESQL request
     * @param exchangeId the exchange ID
     */
    public RemoteSink newRemoteSink(Task parentTask, String exchangeId, DiscoveryNode remoteNode) {
        return new TransportRemoteSink(transportService, remoteNode, parentTask, exchangeId);
    }

    record TransportRemoteSink(TransportService transportService, DiscoveryNode node, Task parentTask, String exchangeId)
        implements
            RemoteSink {

        @Override
        public void fetchPageAsync(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener) {
            transportService.sendChildRequest(
                node,
                EXCHANGE_ACTION_NAME,
                new ExchangeRequest(exchangeId, allSourcesFinished),
                parentTask,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, ExchangeResponse::new)
            );
        }
    }
}
