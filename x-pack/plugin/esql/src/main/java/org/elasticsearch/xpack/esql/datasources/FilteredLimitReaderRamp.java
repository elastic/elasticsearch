/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * Shared filtered-LIMIT reader ramp used by the file and connector source factories.
 * Starts one producer, then doubles the cap when a unit EOFs or after {@link #PROGRESS_PAGES}
 * driver-processed pages while the observed limiter still has remaining rows. Pages are the
 * progress signal rather than elapsed time because I/O latency is too noisy to pick a stable
 * delay.
 * <p>
 * {@link #PROGRESS_PAGES} is provisional. Measure K=1/4/16 with a local uncommitted override of
 * this constant (never a committed sysprop or pragma), pin the smallest K that keeps a dense
 * first page at cap 1, then drop this note.
 * <p>
 * Page-progress start/skip lists are dispatched on the factory executor. If that executor
 * drops the task without throwing (node shutdown), parked producers strand. Same as the
 * existing producer submit path.
 */
final class FilteredLimitReaderRamp<T> {

    static final int PROGRESS_PAGES = 4;

    enum Submit {
        START,
        PARK,
        SKIP
    }

    record Drain<T>(List<T> toStart, List<T> toSkip) {
        boolean isEmpty() {
            return toStart.isEmpty() && toSkip.isEmpty();
        }
    }

    @SuppressWarnings("rawtypes")
    private static final Drain EMPTY = new Drain<>(List.of(), List.of());

    private final Object lock = new Object();
    private final BooleanSupplier noFurther;
    private final Predicate<T> dropUnit;
    private int running;
    private int cap = AsyncExternalSourceOperatorFactory.FILTERED_LIMIT_READER_RAMP;
    private int pagesSinceRamp;
    private int progressPages = PROGRESS_PAGES;
    private final ArrayDeque<T> parked = new ArrayDeque<>();
    private volatile boolean hasParked;

    FilteredLimitReaderRamp(BooleanSupplier noFurther, Predicate<T> dropUnit) {
        this.noFurther = Objects.requireNonNull(noFurther, "noFurther");
        this.dropUnit = Objects.requireNonNull(dropUnit, "dropUnit");
    }

    void setProgressPages(int pages) {
        if (pages < 1) {
            throw new IllegalArgumentException("filtered limit ramp progress pages must be at least 1, got: " + pages);
        }
        synchronized (lock) {
            this.progressPages = pages;
        }
    }

    int cap() {
        synchronized (lock) {
            return cap;
        }
    }

    int parkedCount() {
        synchronized (lock) {
            return parked.size();
        }
    }

    Submit submit(T item, boolean skipNow) {
        synchronized (lock) {
            if (skipNow) {
                return Submit.SKIP;
            }
            if (running < cap) {
                running++;
                return Submit.START;
            }
            parked.add(item);
            hasParked = true;
            return Submit.PARK;
        }
    }

    Drain<T> onProducerFinished() {
        List<T> toStart = new ArrayList<>();
        List<T> toSkip = new ArrayList<>();
        synchronized (lock) {
            running--;
            drainLocked(toStart, toSkip);
        }
        return new Drain<>(toStart, toSkip);
    }

    Drain<T> doubleCap() {
        List<T> toStart = new ArrayList<>();
        List<T> toSkip = new ArrayList<>();
        synchronized (lock) {
            if (noFurther.getAsBoolean()) {
                drainLocked(toStart, toSkip);
            } else {
                doubleCapAndDrainLocked(toStart, toSkip);
            }
        }
        return new Drain<>(toStart, toSkip);
    }

    Drain<T> onProcessedPage() {
        if (hasParked == false) {
            return emptyDrain();
        }
        synchronized (lock) {
            if (parked.isEmpty()) {
                hasParked = false;
                return emptyDrain();
            }
            if (noFurther.getAsBoolean()) {
                List<T> toStart = new ArrayList<>();
                List<T> toSkip = new ArrayList<>();
                drainLocked(toStart, toSkip);
                return new Drain<>(toStart, toSkip);
            }
            pagesSinceRamp++;
            if (pagesSinceRamp < progressPages) {
                return emptyDrain();
            }
            List<T> toStart = new ArrayList<>();
            List<T> toSkip = new ArrayList<>();
            doubleCapAndDrainLocked(toStart, toSkip);
            return new Drain<>(toStart, toSkip);
        }
    }

    private void doubleCapAndDrainLocked(List<T> toStart, List<T> toSkip) {
        pagesSinceRamp = 0;
        if (cap < Integer.MAX_VALUE / 2) {
            cap *= 2;
        } else {
            cap = Integer.MAX_VALUE;
        }
        drainLocked(toStart, toSkip);
    }

    private void drainLocked(List<T> toStart, List<T> toSkip) {
        if (noFurther.getAsBoolean()) {
            while (parked.isEmpty() == false) {
                toSkip.add(parked.poll());
            }
            hasParked = false;
            return;
        }
        while (running < cap && parked.isEmpty() == false) {
            T item = parked.poll();
            if (dropUnit.test(item) || noFurther.getAsBoolean()) {
                toSkip.add(item);
                continue;
            }
            running++;
            toStart.add(item);
        }
        hasParked = parked.isEmpty() == false;
    }

    @SuppressWarnings("unchecked")
    private static <T> Drain<T> emptyDrain() {
        return EMPTY;
    }
}
