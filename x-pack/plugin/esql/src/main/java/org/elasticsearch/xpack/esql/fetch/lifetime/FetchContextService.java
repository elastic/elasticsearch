/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The reader contexts a node keeps open for the fetch phase of ES|QL queries.
 * <p>
 * The count of open contexts follows their real lifetime: it grows when a request opens one and shrinks when the
 * context closes, whoever freed it. Contexts of the fetch phase outlive their request, so they get their own limit,
 * {@link #MAX_OPEN_CONTEXTS}.
 */
public final class FetchContextService {
    /**
     * The most fetch contexts one node keeps open. A data node request that would go beyond fails with status 429, and the
     * coordinator retries its shards on other copies.
     */
    public static final Setting<Integer> MAX_OPEN_CONTEXTS = Setting.intSetting(
        "esql.fetch.max_open_contexts",
        10_000,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final SearchService searchService;
    private final ThreadPool threadPool;
    private final AtomicInteger openContexts = new AtomicInteger();
    private volatile int maxOpenContexts;

    public FetchContextService(SearchService searchService, ThreadPool threadPool, ClusterSettings clusterSettings) {
        this.searchService = searchService;
        this.threadPool = threadPool;
        clusterSettings.initializeAndWatch(MAX_OPEN_CONTEXTS, max -> maxOpenContexts = max);
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

    SearchService searchService() {
        return searchService;
    }

    ThreadContext threadContext() {
        return threadPool.getThreadContext();
    }

    Executor generic() {
        return threadPool.generic();
    }
}
