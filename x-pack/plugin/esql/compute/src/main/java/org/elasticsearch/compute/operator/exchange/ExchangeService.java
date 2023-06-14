/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ExchangeService} is responsible for exchanging pages between exchange sinks and sources on the same or different nodes.
 * It holds a map of {@link ExchangeSourceHandler} and {@link ExchangeSinkHandler} instances for each node in the cluster.
 * To connect exchange sources to exchange sinks, use the {@link ExchangeSourceHandler#addRemoteSink(RemoteSink, int)} method.
 * TODO:
 * - Add a reaper that removes/closes inactive sinks (i.e., no sink, source for more than 30 seconds)
 */
public final class ExchangeService extends AbstractLifecycleComponent {
    // TODO: Make this a child action of the data node transport to ensure that exchanges
    // are accessed only by the user initialized the session.
    public static final String EXCHANGE_ACTION_NAME = "internal:data/read/esql/exchange";

    private static final Logger LOGGER = LogManager.getLogger(ExchangeService.class);
    /**
     * An interval for an exchange request to wait before timing out when the corresponding sink handler doesn't exist.
     * This timeout provides an extra safeguard to ensure the pending requests will always be completed and clean up if
     * data-node requests don't arrive or fail or the corresponding sink handlers are already completed and removed.
     */
    public static final Setting<TimeValue> INACTIVE_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "esql.exchange.inactive_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    private final ThreadPool threadPool;

    private final Map<String, ExchangeSinkHandler> sinks = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PendingGroup> pendingGroups = ConcurrentCollections.newConcurrentMap();
    private final Map<String, ExchangeSourceHandler> sources = ConcurrentCollections.newConcurrentMap();

    private final PendingRequestNotifier pendingRequestNotifier;

    public ExchangeService(Settings settings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.pendingRequestNotifier = new PendingRequestNotifier(LOGGER, threadPool, INACTIVE_TIMEOUT_SETTING.get(settings));
    }

    public void registerTransportHandler(TransportService transportService) {
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
        final PendingGroup pendingGroup = pendingGroups.get(exchangeId);
        if (pendingGroup != null) {
            pendingGroup.onReady(sinkHandler);
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
    public ExchangeSourceHandler createSourceHandler(String exchangeId, int maxBufferSize, String fetchExecutor) {
        ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(maxBufferSize, threadPool.executor(fetchExecutor));
        if (sources.putIfAbsent(exchangeId, sourceHandler) != null) {
            throw new IllegalStateException("source exchanger for id [" + exchangeId + "] already exists");
        }
        sourceHandler.addCompletionListener(ActionListener.releasing(() -> sources.remove(exchangeId)));
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

    private class ExchangeTransportAction implements TransportRequestHandler<ExchangeRequest> {
        @Override
        public void messageReceived(ExchangeRequest request, TransportChannel channel, Task task) {
            final String exchangeId = request.exchangeId();
            final ChannelActionListener<ExchangeResponse> listener = new ChannelActionListener<>(channel);
            ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
            if (sinkHandler != null) {
                sinkHandler.fetchPageAsync(request.sourcesFinished(), listener);
            } else {
                // If a data-node request arrives after an exchange request, we add the listener to the pending list. This allows the
                // data-node request to link the pending listeners with its exchange sink handler when it arrives. We also register the
                // listener to the task cancellation in case the data-node request never arrives due to a network issue or rejection.
                PendingGroup pendingGroup = pendingGroups.compute(exchangeId, (k, group) -> {
                    if (group != null && group.tryIncRef()) {
                        return group;
                    } else {
                        return new PendingGroup(exchangeId);
                    }
                });
                var pendingRequest = new PendingRequest(threadPool.relativeTimeInMillis(), request, pendingGroup::decRef, listener);
                pendingGroup.addRequest(pendingRequest);
                CancellableTask cancellableTask = (CancellableTask) task;
                cancellableTask.addListener(() -> {
                    assert cancellableTask.isCancelled();
                    if (pendingRequest.tryAcquire()) {
                        cancellableTask.notifyIfCancelled(listener);
                    }
                });
                // If the data-node request arrived while we were adding the request to the pending group,
                // we must complete the pending group with the newly created sink handler.
                sinkHandler = sinks.get(exchangeId);
                if (sinkHandler != null) {
                    pendingGroup.onReady(sinkHandler);
                }
            }
        }
    }

    private static class PendingRequest {
        final long addedInMillis;
        final ExchangeRequest request;
        final Releasable onAcquired;
        final ActionListener<ExchangeResponse> listener;
        final AtomicBoolean acquired = new AtomicBoolean();

        PendingRequest(long addedInMillis, ExchangeRequest request, Releasable onAcquired, ActionListener<ExchangeResponse> listener) {
            this.addedInMillis = addedInMillis;
            this.request = request;
            this.onAcquired = onAcquired;
            this.listener = listener;
        }

        boolean tryAcquire() {
            if (acquired.compareAndSet(false, true)) {
                onAcquired.close();
                return true;
            } else {
                return false;
            }
        }
    }

    final class PendingGroup extends AbstractRefCounted {
        private final Queue<PendingRequest> requests = ConcurrentCollections.newQueue();
        private final String exchangeId;

        PendingGroup(String exchangeId) {
            this.exchangeId = exchangeId;
        }

        @Override
        protected void closeInternal() {
            pendingGroups.computeIfPresent(exchangeId, (k, group) -> {
                if (group == PendingGroup.this) {
                    return null;
                } else {
                    return group;
                }
            });
        }

        void addRequest(PendingRequest request) {
            requests.add(request);
        }

        void onReady(ExchangeSinkHandler handler) {
            PendingRequest r;
            while ((r = requests.poll()) != null) {
                if (r.tryAcquire()) {
                    handler.fetchPageAsync(r.request.sourcesFinished(), r.listener);
                }
            }
        }

        void onTimeout(long nowInMillis, TimeValue keepAlive) {
            Iterator<PendingRequest> it = requests.iterator();
            while (it.hasNext()) {
                PendingRequest r = it.next();
                if (r.addedInMillis + keepAlive.millis() < nowInMillis && r.tryAcquire()) {
                    r.listener.onResponse(new ExchangeResponse(null, false));
                    it.remove();
                }
            }
        }
    }

    final class PendingRequestNotifier extends AbstractAsyncTask {
        PendingRequestNotifier(Logger logger, ThreadPool threadPool, TimeValue interval) {
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
            TimeValue keepAlive = getInterval();
            long nowInMillis = threadPool.relativeTimeInMillis();
            for (PendingGroup group : pendingGroups.values()) {
                group.onTimeout(nowInMillis, keepAlive);
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
        // TODO: assert sinks are removed when adding the reaper
        return sources.isEmpty() && pendingGroups.isEmpty();
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        pendingRequestNotifier.close();
    }

    @Override
    protected void doClose() {
        doStop();
    }

    @Override
    public String toString() {
        return "ExchangeService{"
            + "pending="
            + pendingGroups.keySet()
            + ", sinks="
            + sinks.keySet()
            + ", sources="
            + sources.keySet()
            + '}';
    }
}
