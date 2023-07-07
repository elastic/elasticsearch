/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/**
 * {@link ExchangeService} is responsible for exchanging pages between exchange sinks and sources on the same or different nodes.
 * It holds a map of {@link ExchangeSourceHandler} and {@link ExchangeSinkHandler} instances for each node in the cluster.
 * To connect exchange sources to exchange sinks, use the {@link ExchangeSourceHandler#addRemoteSink(RemoteSink, int)} method.
 */
public final class ExchangeService extends AbstractLifecycleComponent {
    // TODO: Make this a child action of the data node transport to ensure that exchanges
    // are accessed only by the user initialized the session.
    public static final String EXCHANGE_ACTION_NAME = "internal:data/read/esql/exchange";

    private static final String OPEN_EXCHANGE_ACTION_NAME = "internal:data/read/esql/open_exchange";

    /**
     * The time interval for an exchange sink handler to be considered inactive and subsequently
     * removed from the exchange service if no sinks are attached (i.e., no computation uses that sink handler).
     */
    public static final String INACTIVE_SINKS_INTERVAL_SETTING = "esql.exchange.sink_inactive_interval";

    private static final Logger LOGGER = LogManager.getLogger(ExchangeService.class);

    private final ThreadPool threadPool;

    private final Map<String, ExchangeSinkHandler> sinks = ConcurrentCollections.newConcurrentMap();
    private final Map<String, ExchangeSourceHandler> sources = ConcurrentCollections.newConcurrentMap();

    private final InactiveSinksReaper inactiveSinksReaper;

    public ExchangeService(Settings settings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        final var inactiveInterval = settings.getAsTime(INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMinutes(5));
        this.inactiveSinksReaper = new InactiveSinksReaper(LOGGER, threadPool, inactiveInterval);
    }

    public void registerTransportHandler(TransportService transportService) {
        transportService.registerRequestHandler(
            EXCHANGE_ACTION_NAME,
            ThreadPool.Names.SAME,
            ExchangeRequest::new,
            new ExchangeTransportAction()
        );
        transportService.registerRequestHandler(
            OPEN_EXCHANGE_ACTION_NAME,
            ThreadPool.Names.SAME,
            OpenExchangeRequest::new,
            new OpenExchangeRequestHandler()
        );
    }

    /**
     * Creates an {@link ExchangeSinkHandler} for the specified exchange id.
     *
     * @throws IllegalStateException if a sink handler for the given id already exists
     */
    ExchangeSinkHandler createSinkHandler(String exchangeId, int maxBufferSize) {
        ExchangeSinkHandler sinkHandler = new ExchangeSinkHandler(maxBufferSize, threadPool::relativeTimeInMillis);
        if (sinks.putIfAbsent(exchangeId, sinkHandler) != null) {
            throw new IllegalStateException("sink exchanger for id [" + exchangeId + "] already exists");
        }
        return sinkHandler;
    }

    /**
     * Returns an exchange sink handler for the given id.
     */
    public ExchangeSinkHandler getSinkHandler(String exchangeId) {
        ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
        if (sinkHandler == null) {
            throw new ResourceNotFoundException("sink exchanger for id [{}] doesn't exist", exchangeId);
        }
        return sinkHandler;
    }

    /**
     * Removes the exchange sink handler associated with the given exchange id.
     */
    public void finishSinkHandler(String exchangeId, Exception failure) {
        final ExchangeSinkHandler sinkHandler = sinks.remove(exchangeId);
        if (sinkHandler != null) {
            if (failure != null) {
                sinkHandler.onFailure(failure);
            }
            assert sinkHandler.isFinished() : "Exchange sink " + exchangeId + " wasn't finished yet";
        }
    }

    /**
     * Creates an {@link ExchangeSourceHandler} for the specified exchange id.
     *
     * @throws IllegalStateException if a source handler for the given id already exists
     */
    public ExchangeSourceHandler createSourceHandler(String exchangeId, int maxBufferSize, String fetchExecutor) {
        ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(maxBufferSize, threadPool.executor(fetchExecutor));
        if (sources.putIfAbsent(exchangeId, sourceHandler) != null) {
            throw new IllegalStateException("source exchanger for id [" + exchangeId + "] already exists");
        }
        sourceHandler.addCompletionListener(ActionListener.releasing(() -> sources.remove(exchangeId)));
        return sourceHandler;
    }

    /**
     * Opens a remote sink handler on the remote node for the given session ID.
     */
    public static void openExchange(
        TransportService transportService,
        DiscoveryNode targetNode,
        String sessionId,
        int exchangeBuffer,
        ActionListener<Void> listener
    ) {
        transportService.sendRequest(
            targetNode,
            OPEN_EXCHANGE_ACTION_NAME,
            new OpenExchangeRequest(sessionId, exchangeBuffer),
            new ActionListenerResponseHandler<>(listener.map(unused -> null), in -> TransportResponse.Empty.INSTANCE)
        );
    }

    private static class OpenExchangeRequest extends TransportRequest {
        private final String sessionId;
        private final int exchangeBuffer;

        OpenExchangeRequest(String sessionId, int exchangeBuffer) {
            this.sessionId = sessionId;
            this.exchangeBuffer = exchangeBuffer;
        }

        OpenExchangeRequest(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.exchangeBuffer = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeVInt(exchangeBuffer);
        }
    }

    private class OpenExchangeRequestHandler implements TransportRequestHandler<OpenExchangeRequest> {
        @Override
        public void messageReceived(OpenExchangeRequest request, TransportChannel channel, Task task) throws Exception {
            createSinkHandler(request.sessionId, request.exchangeBuffer);
            channel.sendResponse(new TransportResponse.Empty());
        }
    }

    private class ExchangeTransportAction implements TransportRequestHandler<ExchangeRequest> {
        @Override
        public void messageReceived(ExchangeRequest request, TransportChannel channel, Task task) {
            final String exchangeId = request.exchangeId();
            ActionListener<ExchangeResponse> listener = new ChannelActionListener<>(channel);
            final ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
            if (sinkHandler == null) {
                listener.onResponse(new ExchangeResponse(null, true));
            } else {
                // the data-node request hasn't arrived yet; use the task framework to cancel the request if needed.
                if (sinkHandler.hasData() == false) {
                    ((CancellableTask) task).addListener(() -> sinkHandler.onFailure(new TaskCancelledException("task cancelled")));
                }
                sinkHandler.fetchPageAsync(request.sourcesFinished(), listener);
            }
        }
    }

    private final class InactiveSinksReaper extends AbstractAsyncTask {
        InactiveSinksReaper(Logger logger, ThreadPool threadPool, TimeValue interval) {
            super(logger, threadPool, interval, true);
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            Lifecycle.State state = lifecycleState();
            return state != Lifecycle.State.STOPPED && state != Lifecycle.State.CLOSED;
        }

        @Override
        protected void runInternal() {
            final TimeValue maxInterval = getInterval();
            final long nowInMillis = threadPool.relativeTimeInMillis();
            for (Map.Entry<String, ExchangeSinkHandler> e : sinks.entrySet()) {
                ExchangeSinkHandler sink = e.getValue();
                if (sink.hasData() && sink.hasListeners()) {
                    continue;
                }
                long elapsed = nowInMillis - sink.lastUpdatedTimeInMillis();
                if (elapsed > maxInterval.millis()) {
                    finishSinkHandler(
                        e.getKey(),
                        new ElasticsearchTimeoutException(
                            "Exchange sink {} has been inactive for {}",
                            e.getKey(),
                            TimeValue.timeValueMillis(elapsed)
                        )
                    );
                }
            }
        }
    }

    /**
     * Creates a new {@link RemoteSink} that fetches pages from an exchange sink located on the remote node.
     *
     * @param parentTask       the parent task that initialized the ESQL request
     * @param exchangeId       the exchange ID
     * @param transportService the transport service
     * @param remoteNode       the node where the remote exchange sink is located
     */
    public RemoteSink newRemoteSink(Task parentTask, String exchangeId, TransportService transportService, DiscoveryNode remoteNode) {
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

    // For testing
    public boolean isEmpty() {
        return sources.isEmpty() && sinks.isEmpty();
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        inactiveSinksReaper.close();
    }

    @Override
    protected void doClose() {
        doStop();
    }

    @Override
    public String toString() {
        return "ExchangeService{" + "sinks=" + sinks.keySet() + ", sources=" + sources.keySet() + '}';
    }
}
