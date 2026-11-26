/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.read.dv;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Closes the forward scans that nobody uses any more. An open scan reserves a share of the ES|QL reader's byte budget and keeps its
 * prefetched column chunks until it is closed, and a Lucene iterator has no close hook, so an iterator that is abandoned (a finished
 * ES|QL slice or query) leaves its scan open. Left alone, the abandoned scans use up the budget and the next read waits for it forever.
 *
 * <p>A scan in use is touched on every page it loads, so a scan that was not touched for {@link #IDLE_NANOS} has no reader. Eviction
 * happens when scans are opened, when scans are touched (at most once for each {@link #SWEEP_EVERY_NANOS}, whichever thread comes first)
 * and on the timer of the plugin, see {@link #sweep()}. Evicting is always safe: the window of the evicted scan only reopens it at its
 * next load. The registry is shared by all segments of the node, because the budget is.
 *
 * <p>A touch takes no lock: a registry-wide lock made the driver threads of one query wait for each other. The window keeps the time of
 * its last touch, and only a scan that opens or closes, and the sweep, take the lock of the set of open scans.
 */
public final class ScanRegistry {

    public static final ScanRegistry INSTANCE = new ScanRegistry();

    private static final long IDLE_NANOS = TimeUnit.SECONDS.toNanos(2);

    /** The least time between two sweeps that touches start. */
    private static final long SWEEP_EVERY_NANOS = IDLE_NANOS / 4;

    /** The windows with an open scan, guarded by itself. */
    private final Set<ColumnWindow> open = new HashSet<>();

    /** The time, as {@link System#nanoTime()}, from which a touch may sweep. */
    private final AtomicLong nextTouchSweep = new AtomicLong(System.nanoTime());

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
        Thread thread = Thread.currentThread();
        long now = System.nanoTime();
        List<ColumnWindow> victims = new ArrayList<>();
        List<OwnScan> ownScans = new ArrayList<>();
        synchronized (open) {
            for (Iterator<ColumnWindow> it = open.iterator(); it.hasNext();) {
                ColumnWindow window = it.next();
                long lastTouch = window.lastTouchNanos();
                if (now - lastTouch > IDLE_NANOS) {
                    it.remove();
                    victims.add(window);
                } else if (window != opening && window.readsSameColumnAs(opening) && window.scanOpenedBy(thread)) {
                    ownScans.add(new OwnScan(window, lastTouch));
                }
            }
        }
        int excess = ownScans.size() - (MAX_SCANS_PER_THREAD_AND_COLUMN - 1);
        if (excess > 0) {
            // the oldest first
            ownScans.sort(Comparator.comparingLong(OwnScan::lastTouch));
            for (int i = 0; i < excess; i++) {
                victims.add(ownScans.get(i).window());
            }
        }
        closeAll(victims);
    }

    /** A scan of the calling thread, with its last touch time. */
    private record OwnScan(ColumnWindow window, long lastTouch) {}

    /** Closes the scans that nobody used for the idle time. The plugin calls this on a timer. */
    public void sweep() {
        closeAll(evictIdle());
    }

    /** Records that the window's scan is open, and in use now. */
    void opened(ColumnWindow window) {
        window.touchedAt(System.nanoTime());
        synchronized (open) {
            open.add(window);
        }
    }

    /** Records that the window's open scan is in use. */
    void touch(ColumnWindow window) {
        long now = System.nanoTime();
        window.touchedAt(now);
        long next = nextTouchSweep.get();
        if (now - next >= 0 && nextTouchSweep.compareAndSet(next, now + SWEEP_EVERY_NANOS)) {
            closeAll(evictIdle());
        }
    }

    void remove(ColumnWindow window) {
        synchronized (open) {
            open.remove(window);
        }
    }

    /** Closes the scans of the windows that the producer owns, when the segment closes. */
    void closeAllOf(ParquetDocValuesProducer producer) {
        List<ColumnWindow> owned = new ArrayList<>();
        synchronized (open) {
            for (ColumnWindow window : open) {
                if (window.ownedBy(producer)) {
                    owned.add(window);
                }
            }
        }
        closeAll(owned);
    }

    /** Takes the windows with an idle scan out of the set. */
    private List<ColumnWindow> evictIdle() {
        List<ColumnWindow> idle = new ArrayList<>();
        long now = System.nanoTime();
        synchronized (open) {
            for (Iterator<ColumnWindow> it = open.iterator(); it.hasNext();) {
                ColumnWindow window = it.next();
                if (now - window.lastTouchNanos() > IDLE_NANOS) {
                    it.remove();
                    idle.add(window);
                }
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
