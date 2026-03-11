/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory store for paginated ES|QL query results on the coordinator node.
 * Each cursor holds the re-chunked pages of a completed query and is evicted
 * after its TTL expires or when explicitly closed.
 */
public class EsqlCursorStore extends AbstractLifecycleComponent {

    public static final TimeValue DEFAULT_CURSOR_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
    private static final TimeValue REAP_INTERVAL = TimeValue.timeValueSeconds(30);

    private final ConcurrentHashMap<String, CursorState> cursors = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable reaper;

    public EsqlCursorStore(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Stores paginated results and returns a unique cursor ID.
     */
    public String store(CursorState state) {
        String cursorId = UUIDs.randomBase64UUID();
        cursors.put(cursorId, state);
        return cursorId;
    }

    /**
     * Retrieves the cursor state, or null if expired or not found.
     */
    public CursorState get(String cursorId) {
        CursorState state = cursors.get(cursorId);
        if (state == null) {
            return null;
        }
        if (state.isExpired(threadPool.absoluteTimeInMillis())) {
            delete(cursorId);
            return null;
        }
        return state;
    }

    /**
     * Explicitly closes a cursor and releases its resources.
     * @return true if the cursor existed and was removed
     */
    public boolean delete(String cursorId) {
        CursorState removed = cursors.remove(cursorId);
        if (removed != null) {
            removed.close();
            return true;
        }
        return false;
    }

    /**
     * Updates the expiration time of a cursor.
     */
    public void updateKeepAlive(String cursorId, TimeValue keepAlive) {
        CursorState state = cursors.get(cursorId);
        if (state != null) {
            state.expirationTimeMillis = threadPool.absoluteTimeInMillis() + keepAlive.getMillis();
        }
    }

    /**
     * Removes all expired cursors.
     */
    void reap() {
        long now = threadPool.absoluteTimeInMillis();
        Iterator<Map.Entry<String, CursorState>> it = cursors.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CursorState> entry = it.next();
            if (entry.getValue().isExpired(now)) {
                it.remove();
                entry.getValue().close();
            }
        }
    }

    int size() {
        return cursors.size();
    }

    @Override
    protected void doStart() {
        reaper = threadPool.scheduleWithFixedDelay(this::reap, REAP_INTERVAL, threadPool.generic());
    }

    @Override
    protected void doStop() {
        if (reaper != null) {
            reaper.cancel();
        }
    }

    @Override
    protected void doClose() {
        for (CursorState state : cursors.values()) {
            state.close();
        }
        cursors.clear();
    }

    /**
     * Holds the paginated results for a single cursor.
     */
    public static class CursorState {
        private final List<ColumnInfoImpl> columns;
        private final List<Page> pages;
        private final int totalRows;
        private final ZoneId zoneId;
        private final boolean columnar;
        volatile long expirationTimeMillis;

        public CursorState(
            List<ColumnInfoImpl> columns,
            List<Page> pages,
            int totalRows,
            long expirationTimeMillis,
            ZoneId zoneId,
            boolean columnar
        ) {
            this.columns = columns;
            this.pages = pages;
            this.totalRows = totalRows;
            this.expirationTimeMillis = expirationTimeMillis;
            this.zoneId = zoneId;
            this.columnar = columnar;
        }

        public List<ColumnInfoImpl> columns() {
            return columns;
        }

        public List<Page> pages() {
            return pages;
        }

        public int totalRows() {
            return totalRows;
        }

        public ZoneId zoneId() {
            return zoneId;
        }

        public boolean columnar() {
            return columnar;
        }

        public int pageCount() {
            return pages.size();
        }

        boolean isExpired(long nowMillis) {
            return nowMillis >= expirationTimeMillis;
        }

        void close() {
            Releasables.close(() -> pages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks)));
        }
    }
}
