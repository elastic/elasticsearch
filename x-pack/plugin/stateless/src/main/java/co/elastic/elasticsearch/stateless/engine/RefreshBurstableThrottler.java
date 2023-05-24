/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A refresh throttler that allows accumulating unused refreshes (here referred to as credit) to be used for handling
 * a burst of refresh requests. Credit and bursting is defined based on throttling intervals. For each throttling
 * interval (e.g. 5s) we allocate one refresh credit. Therefore, within each throttling interval at least one refresh
 * can happen, and any refresh after that (within the same 5s interval) would need to use accumulated unused credits (if
 * any) for bursting or will be throttled.
 */
public class RefreshBurstableThrottler implements RefreshThrottler {
    private static final Logger logger = LogManager.getLogger(RefreshBurstableThrottler.class);

    public static final TimeValue BUDGET_INTERVAL = TimeValue.timeValueHours(1);
    // TODO: probably we should move throttling_interval to a node setting.
    public static final TimeValue THROTTLING_INTERVAL = TimeValue.timeValueSeconds(5);
    public static final long CREDITS_PER_THROTTLING_INTERVAL = 1;

    private final Object mutex = new Object();
    private final long throttlingIntervalMillis;
    private final long maxCredit;
    private final Consumer<Request> refresh;
    private final ThreadPool threadPool;
    private final long creditPerInterval;
    private final LongSupplier relativeTimeSupplier;
    private final long firstIntervalStartMillis;
    private long credit;
    private List<Request> pendingRequests = new ArrayList<>();
    private long lastRefreshMillis;
    private long lastCreditUpdate = -1;
    // TODO: Should we add timestamps to these and keep only last X hours?
    // TODO: Should these stats be aggregated/exposed somewhere else?
    private final ConcurrentMap<String, Long> throttledPerSource = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> acceptedPerSource = new ConcurrentHashMap<>();

    public RefreshBurstableThrottler(
        Consumer<Request> refresh,
        TimeValue throttlingInterval,
        long creditPerInterval,
        long initialCredit,
        long maxCredit,
        LongSupplier relativeTimeSupplierInMillis,
        ThreadPool threadPool
    ) {
        this.refresh = refresh;
        this.throttlingIntervalMillis = throttlingInterval.millis();
        this.creditPerInterval = creditPerInterval;
        this.relativeTimeSupplier = relativeTimeSupplierInMillis;
        this.threadPool = threadPool;
        this.maxCredit = maxCredit;
        credit = Math.min(maxCredit, initialCredit + creditPerInterval);
        firstIntervalStartMillis = relativeTimeSupplierInMillis.getAsLong();
        // we will calculate accumulated credit since last refresh
        lastRefreshMillis = relativeTimeSupplierInMillis.getAsLong();
    }

    @Override
    public boolean maybeThrottle(Request request) {
        if (maybeRefresh(request)) {
            updateAcceptedStats(request.source());
            return false;
        }
        logger.debug("Refresh request with source '{}' throttled.", request.source());
        updateThrottledStats(request.source());
        return true;
    }

    // Attempts to run a refresh if there is at least one credit available. Returns true if a refresh is run.
    private boolean maybeRefresh(Request request) {
        List<Request> requestsToRun;
        synchronized (mutex) {
            pendingRequests.add(request);
            if (pendingRequests.size() > 1) {
                // There is already a pending request which means we have scheduled a refresh for the next interval.
                return false;
            }
            long relativeTimeMillis = relativeTimeSupplier.getAsLong();
            updateCredit(relativeTimeMillis);
            requestsToRun = getRefreshRequestsToRun(relativeTimeMillis);
            if (requestsToRun.isEmpty()) {
                assert pendingRequests.size() == 1;
                // We are throttling the added request. Schedule a refresh to handle the throttled refresh requests in
                // the next interval where we are sure there will be at least one new credit available.
                scheduleRefresh();
            }
        }
        if (requestsToRun.isEmpty() == false) {
            refresh(requestsToRun);
            return true;
        }
        return false;
    }

    private void scheduleRefresh() {
        // TODO: currently we are waiting a full throttling interval to handle pending (throttled)
        // refresh requests. We could however reduce this worst case by calculating a smaller delay, e.g. based on the
        // last refresh timestamp.
        threadPool.scheduleUnlessShuttingDown(
            TimeValue.timeValueMillis(throttlingIntervalMillis),
            ThreadPool.Names.REFRESH,
            this::runPendingRequests
        );
    }

    private List<Request> getRefreshRequestsToRun(long relativeTimeMillis) {
        assert Thread.holdsLock(mutex);
        List<Request> requestsToAccept = List.of();
        if (credit > 0 && pendingRequests.isEmpty() == false) {
            credit--;
            requestsToAccept = pendingRequests;
            pendingRequests = new ArrayList<>();
            lastRefreshMillis = relativeTimeMillis;
        }
        assert credit >= 0;
        return requestsToAccept;
    }

    /**
     * Updates the available credit at a given interval (based on {@code relativeTimeMillis} and the last refresh)
     */
    private void updateCredit(long relativeTimeMillis) {
        assert Thread.holdsLock(mutex);
        long curIntervalNo = getIntervalNo(relativeTimeMillis);
        if (curIntervalNo <= lastCreditUpdate) {
            return;
        }
        lastCreditUpdate = curIntervalNo;
        long lastRefreshIntervalNo = getIntervalNo(lastRefreshMillis);
        long unusedCredit = (curIntervalNo - lastRefreshIntervalNo) * creditPerInterval;
        incrementCredit(unusedCredit);
    }

    private void runPendingRequests() {
        List<Request> requestsToRun;
        synchronized (mutex) {
            long relativeTimeMillis = relativeTimeSupplier.getAsLong();
            updateCredit(relativeTimeMillis);
            // We have promised to run a refresh in this interval and we do so.
            // We're avoiding credit < 0 here since due to cached relative time there might be cases where we miss the new
            // credit due to the cached time and running the promised refresh would result in negative credit.
            // For simplicity, we just refresh even if no credit is available.
            credit = Math.max(0, credit - 1);
            requestsToRun = pendingRequests;
            pendingRequests = new ArrayList<>();
            lastRefreshMillis = relativeTimeMillis;
        }
        assert requestsToRun.isEmpty() == false;
        refresh(requestsToRun);
    }

    private void incrementCredit(long i) {
        assert Thread.holdsLock(mutex);
        credit = Math.min(maxCredit, credit + i);
        assert credit <= maxCredit;
    }

    private void refresh(List<Request> requests) {
        assert requests.isEmpty() == false;
        String source = requests.size() > 1 ? "batched" : requests.get(0).source();
        ActionListener<Engine.RefreshResult> listener = ActionListener.wrap(refreshResult -> {
            if (refreshResult.refreshed() == false) {
                synchronized (mutex) {
                    // Return the unused credit
                    incrementCredit(1);
                }
            }
            requests.forEach(request -> request.listener().onResponse(refreshResult));
        }, e -> requests.forEach(request -> request.listener().onFailure(e)));
        refresh.accept(new Request(source, listener));
    }

    private void updateAcceptedStats(String source) {
        acceptedPerSource.compute(source, (s, count) -> count == null ? 1 : count + 1);
    }

    private void updateThrottledStats(String source) {
        throttledPerSource.compute(source, (s, count) -> count == null ? 1 : count + 1);
    }

    public Map<String, Long> getThrottledPerSourceStats() {
        return Map.copyOf(throttledPerSource);
    }

    public Map<String, Long> getAcceptedPerSourceStats() {
        return Map.copyOf(acceptedPerSource);
    }

    // Calculates which throttling interval number a given timestamp falls into (starting with 0).
    // We enumerate all intervals since the first interval start time.
    // package private for testing
    long getIntervalNo(long timeMillis) {
        assert timeMillis - firstIntervalStartMillis >= 0;
        return (timeMillis - firstIntervalStartMillis) / throttlingIntervalMillis;
    }

    // package private for testing
    long getCredit() {
        return credit;
    }
}
