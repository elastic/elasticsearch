/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The reader contexts of the fetch phase of ES|QL queries, on both sides.
 * <p>
 * On a data node it counts the open contexts. The count grows when a request opens one and shrinks when the context
 * closes, whoever freed it. Contexts of the fetch phase outlive their request, so they get their own limit,
 * {@link #MAX_OPEN_CONTEXTS}. It also serves the requests of coordinators that free contexts.
 * <p>
 * On a coordinator it keeps one {@link FetchContextLease} per running query.
 */
public final class FetchContextService {
    private static final Logger logger = LogManager.getLogger(FetchContextService.class);

    /**
     * Frees fetch contexts on a data node. Its requests carry the index expressions of the query, so they are authorized
     * like the query's other index actions.
     */
    public static final String FREE_ACTION_NAME = EsqlQueryAction.NAME + "/fetch/free";

    /**
     * The most fetch contexts one node keeps open. A data node request opens one per shard and holds each until it ends.
     * The contexts whose rows survive the node cut then stay open until their coordinator frees them, or until their
     * keep-alive passes if the coordinator never does. Opening one more fails the shard with status 429, and the
     * coordinator retries it on another copy.
     */
    public static final Setting<Integer> MAX_OPEN_CONTEXTS = Setting.intSetting(
        "esql.fetch.max_open_contexts",
        10_000,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How long a data node keeps a fetch context after its last use. The coordinator reads it when a query starts and sends
     * it to the data nodes. It defaults to {@link SearchService#DEFAULT_KEEPALIVE_SETTING}. A value set on its own is at
     * least a second and at most {@link SearchService#MAX_KEEPALIVE_SETTING}. Each data node checks the value it gets
     * against its own limit too, because nodes can have different limits.
     */
    public static final Setting<TimeValue> CONTEXT_KEEP_ALIVE = Setting.timeSetting(
        "esql.fetch.context_keep_alive",
        SearchService.DEFAULT_KEEPALIVE_SETTING,
        new ContextKeepAliveValidator(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Checks only a value set on its own. {@link SearchService} already keeps its default keep-alive within its limit.
     * Without a minimum a context could expire before the coordinator fetches from it.
     */
    static final class ContextKeepAliveValidator implements Setting.Validator<TimeValue> {
        static final TimeValue MIN_KEEP_ALIVE = TimeValue.timeValueSeconds(1);

        @Override
        public void validate(TimeValue value) {}

        @Override
        public void validate(TimeValue value, Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent == false) {
                return;
            }
            if (value.compareTo(MIN_KEEP_ALIVE) < 0) {
                throw new IllegalArgumentException(
                    "[" + CONTEXT_KEEP_ALIVE.getKey() + "] must be at least [" + MIN_KEEP_ALIVE + "], but was [" + value + "]"
                );
            }
            TimeValue max = (TimeValue) settings.get(SearchService.MAX_KEEPALIVE_SETTING);
            if (value.compareTo(max) > 0) {
                throw new IllegalArgumentException(
                    "["
                        + CONTEXT_KEEP_ALIVE.getKey()
                        + "] must be at most ["
                        + max
                        + "], the value of ["
                        + SearchService.MAX_KEEPALIVE_SETTING.getKey()
                        + "], but was ["
                        + value
                        + "]"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(SearchService.MAX_KEEPALIVE_SETTING).iterator();
        }
    }

    private final SearchService searchService;
    private final TransportService transportService;
    private final AtomicInteger openContexts = new AtomicInteger();
    private final Map<Long, FetchContextLease> leases = new ConcurrentHashMap<>();
    private volatile int maxOpenContexts;
    private volatile TimeValue keepAlive;

    public FetchContextService(SearchService searchService, TransportService transportService, ClusterSettings clusterSettings) {
        this.searchService = searchService;
        this.transportService = transportService;
        clusterSettings.initializeAndWatch(MAX_OPEN_CONTEXTS, max -> maxOpenContexts = max);
        clusterSettings.initializeAndWatch(CONTEXT_KEEP_ALIVE, value -> keepAlive = value);
    }

    /**
     * Serves the requests that free fetch contexts on this node, on the generic pool because freeing the last reference to
     * a reader can block. Also closes the lease of a query when its task goes away, whichever action ran the query and
     * however it ended.
     */
    public void registerHandlers() {
        transportService.registerRequestHandler(
            FREE_ACTION_NAME,
            transportService.getThreadPool().generic(),
            FetchFreeRequest::new,
            (request, channel, task) -> free(request, channel)
        );
        transportService.getTaskManager().registerRemovedTaskListener(task -> closeLease(task.getId()));
    }

    /**
     * The fetch contexts of one data node request.
     *
     * @param keepAlive how long a context stays open after its last use
     * @param task      the task of the request, recorded as the creator of its contexts
     */
    public NodeFetchContexts newNodeContexts(TimeValue keepAlive, @Nullable Task task) {
        return new NodeFetchContexts(this, keepAlive, task);
    }

    /**
     * The lease of the query that runs under {@code rootTask}, created by the first execution of the query that opens fetch
     * contexts. Every later execution of the query shares it, because a later stage can fetch rows an earlier one made.
     * The lease closes when the task is cancelled or goes away. Called only while the query runs, because nothing closes
     * a lease created after its task went away.
     */
    public FetchContextLease leaseFor(CancellableTask rootTask) {
        FetchContextLease existing = leases.get(rootTask.getId());
        if (existing != null) {
            return existing;
        }
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        FetchContextLease created = new FetchContextLease(this, rootTask.getId(), keepAlive, threadContext.newRestorableContext(false));
        existing = leases.putIfAbsent(rootTask.getId(), created);
        if (existing != null) {
            return existing;
        }
        // runs at once when the task is already cancelled
        rootTask.addListener(created::close);
        return created;
    }

    /**
     * Frees every context of the query that ran under the task {@code rootTaskId}, if it opened any.
     */
    public void closeLease(long rootTaskId) {
        FetchContextLease lease = leases.get(rootTaskId);
        if (lease != null) {
            lease.close();
        }
    }

    /**
     * The fetch contexts open on this node.
     */
    public int openContexts() {
        return openContexts.get();
    }

    void reserve() {
        int open = openContexts.incrementAndGet();
        if (open > maxOpenContexts) {
            openContexts.decrementAndGet();
            throw new ElasticsearchStatusException(
                "this node holds [{}] fetch contexts, the most that [{}] allows",
                RestStatus.TOO_MANY_REQUESTS,
                open - 1,
                MAX_OPEN_CONTEXTS.getKey()
            );
        }
    }

    void release() {
        int open = openContexts.decrementAndGet();
        assert open >= 0 : "released more fetch contexts than were opened";
    }

    void removeLease(long rootTaskId, FetchContextLease lease) {
        leases.remove(rootTaskId, lease);
    }

    void sendFree(DiscoveryNode node, FetchFreeRequest request) {
        transportService.sendRequest(
            node,
            FREE_ACTION_NAME,
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                ActionListener.wrap(
                    response -> {},
                    // the reaper frees the contexts once their keep-alive passes
                    e -> logger.debug(
                        () -> Strings.format("failed to free [%d] fetch contexts on [%s]", request.contextIds().size(), node),
                        e
                    )
                ),
                in -> ActionResponse.Empty.INSTANCE,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            )
        );
    }

    private void free(FetchFreeRequest request, TransportChannel channel) {
        int freed = freeFetchContexts(request.contextIds(), request);
        logger.debug("freed [{}] of [{}] fetch contexts at the request of their coordinator", freed, request.contextIds().size());
        channel.sendResponse(ActionResponse.Empty.INSTANCE);
    }

    /**
     * Frees the contexts among {@code ids} that {@code request} may look up: fetch contexts its user opened. The others
     * stay open, as if they were gone, so a request can't free a scroll, a point in time or another user's context whose
     * id it learned. One context that can't be freed doesn't keep the others open.
     *
     * @param request a request of the fetch phase. This runs in its thread context, which names its user.
     * @return how many contexts it freed
     */
    public <R extends TransportRequest & FetchContextRequest> int freeFetchContexts(Collection<ShardSearchContextId> ids, R request) {
        int freed = 0;
        for (ShardSearchContextId id : ids) {
            try {
                if (freeFetchContext(id, request)) {
                    freed++;
                }
            } catch (Exception e) {
                logger.debug(() -> Strings.format("failed to free fetch context [%s]", id), e);
            }
        }
        return freed;
    }

    private boolean freeFetchContext(ShardSearchContextId id, TransportRequest request) {
        ReaderContext readerContext;
        try {
            // rejects other users and other kinds of requests as if the context was gone
            readerContext = searchService.findReaderContext(id, request, null);
        } catch (SearchContextMissingException e) {
            return false;
        }
        return FetchContextListener.isFetchContext(readerContext) && searchService.freeReaderContext(id);
    }

    SearchService searchService() {
        return searchService;
    }

    ThreadContext threadContext() {
        return transportService.getThreadPool().getThreadContext();
    }

    Executor generic() {
        return transportService.getThreadPool().generic();
    }
}
