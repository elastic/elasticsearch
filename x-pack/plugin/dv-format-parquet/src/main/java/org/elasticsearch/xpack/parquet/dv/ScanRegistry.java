/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Closes the forward scans that nobody uses any more. An open scan reserves a share of the ES|QL reader's byte budget and keeps its
 * prefetched column chunks until it is closed, and a Lucene iterator has no close hook, so an iterator that is abandoned (a finished
 * ES|QL slice or query) leaves its scan open. Left alone, the abandoned scans use up the budget and the next read waits for it forever.
 *
 * <p>A scan in use is touched on every page it loads, so a scan that was not touched for {@link #IDLE_NANOS} has no reader. Eviction
 * happens when scans are opened and touched, which is when the budget is needed; there is no timer. Evicting is always safe: the window
 * of the evicted scan only reopens it at its next load. The registry is shared by all segments of the node, because the budget is.
 */
public final class ScanRegistry {

    public static final ScanRegistry INSTANCE = new ScanRegistry();

    private static final long IDLE_NANOS = TimeUnit.SECONDS.toNanos(2);

    /** In order of the last touch, the least recently touched first. */
    private final Map<ColumnWindow, Long> lastTouch = new LinkedHashMap<>(16, 0.75f, true);

    private ScanRegistry() {}

    /**
     * How many scans one thread keeps open for one column of one segment. A thread legitimately reads a column through several live
     * iterators: ES|QL evaluates a pushed-down filter such as {@code SearchPhrase != ""} through the iterators of the filter's scorers and
     * the iterator that loads the values, three or four on one thread. If they evicted each other's scan, each of their loads would
     * restart a scan at the first page of the row group and decode every page up to the target again: with a limit of 2, 95% of all scan
     * opens were such restarts, and a grouping query over a filtered string column took 54 s instead of 9 s. The bound is therefore
     * generous, and only keeps the scans that a finished slice leaves behind from piling up; the idle rule closes those too.
     */
    static final int MAX_SCANS_PER_THREAD_AND_COLUMN = 16;

    /**
     * Called before a window opens a scan. It closes the scans that hold the budget this one is about to ask for: the scans that were
     * idle too long, and the oldest scans that the calling thread opened for the same column of the same segment, beyond
     * {@link #MAX_SCANS_PER_THREAD_AND_COLUMN} counting this one. The latter are what a thread leaves behind when it moves to its next
     * ES|QL slice, which reads the same field through new iterators; waiting for the idle timeout would stall the thread on its own
     * leftovers.
     */
    void beforeOpen(ColumnWindow opening) {
        List<ColumnWindow> victims = evictIdle(null);
        Thread thread = Thread.currentThread();
        List<ColumnWindow> candidates = new ArrayList<>();
        synchronized (lastTouch) {
            // oldest first
            for (ColumnWindow window : lastTouch.keySet()) {
                if (window != opening && window.readsSameColumnAs(opening)) {
                    candidates.add(window);
                }
            }
        }
        List<ColumnWindow> ownScans = new ArrayList<>();
        for (ColumnWindow window : candidates) {
            if (window.scanOpenedBy(thread) && victims.contains(window) == false) {
                ownScans.add(window);
            }
        }
        int excess = ownScans.size() - (MAX_SCANS_PER_THREAD_AND_COLUMN - 1);
        for (int i = 0; i < excess; i++) {
            victims.add(ownScans.get(i));
        }
        closeAll(victims);
    }

    /**
     * Closes the scans that nobody used for the idle time. The idle rule otherwise runs only when a scan opens or is touched, so a node
     * that stops reading would keep the row group chunks of its last scans on the heap. The plugin calls this on a timer.
     */
    public void sweep() {
        closeAll(evictIdle(null));
    }

    /** Records that the window's scan is open and in use. */
    void touch(ColumnWindow window) {
        closeAll(evictIdle(window));
    }

    void remove(ColumnWindow window) {
        synchronized (lastTouch) {
            lastTouch.remove(window);
        }
    }

    /** Closes the scans of the windows that the producer owns, when the segment closes. */
    void closeAllOf(ParquetDocValuesProducer producer) {
        List<ColumnWindow> owned = new ArrayList<>();
        synchronized (lastTouch) {
            for (ColumnWindow window : lastTouch.keySet()) {
                if (window.ownedBy(producer)) {
                    owned.add(window);
                }
            }
        }
        closeAll(owned);
    }

    private List<ColumnWindow> evictIdle(ColumnWindow touched) {
        List<ColumnWindow> idle = new ArrayList<>();
        long now = System.nanoTime();
        synchronized (lastTouch) {
            if (touched != null) {
                lastTouch.put(touched, now);
            }
            var it = lastTouch.entrySet().iterator();
            while (it.hasNext()) {
                var entry = it.next();
                if (now - entry.getValue() <= IDLE_NANOS) {
                    // the entries after this one were touched more recently
                    break;
                }
                it.remove();
                idle.add(entry.getKey());
            }
        }
        return idle;
    }

    /** Closes outside the registry lock: a window takes its own lock and then this one, so the reverse order could deadlock. */
    private static void closeAll(List<ColumnWindow> windows) {
        for (ColumnWindow window : windows) {
            window.closeScan();
        }
    }
}
