/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Keeps track of the contents of a file that may not be completely present.
 */
public class SparseFileTracker {

    private static final Comparator<Range> RANGE_START_COMPARATOR = Comparator.comparingLong(r -> r.start);

    /**
     * The byte ranges of the file which are present or pending. These ranges are nonempty, disjoint (and in order) and the non-pending
     * ranges are not contiguous (i.e. contiguous non-pending ranges are merged together). See {@link SparseFileTracker#invariant()} for
     * details.
     */
    private final TreeSet<Range> ranges = new TreeSet<>(RANGE_START_COMPARATOR);

    /**
     * Number of bytes from the start of the file that are present as one continuous range without gaps. Used as an optimization in
     * {@link #waitForRange(ByteRange, ByteRange, ActionListener)} to bypass expensive inspection of {@link #ranges} in some cases.
     */
    private volatile long complete = 0L;

    private final Object mutex = new Object();

    private final String description;

    private final long length;

    /**
     * Number of bytes that were initially present in the case where the sparse file tracker was initialized with some completed ranges.
     * See {@link #SparseFileTracker(String, long, SortedSet)}
     */
    private final long initialLength;

    /**
     * Creates a new empty {@link SparseFileTracker}
     *
     * @param description a description for the sparse file tracker
     * @param length      the length of the file tracked by the sparse file tracker
     */
    public SparseFileTracker(String description, long length) {
        this(description, length, Collections.emptySortedSet());
    }

    /**
     * Creates a {@link SparseFileTracker} with some ranges already present
     *
     * @param description a description for the sparse file tracker
     * @param length      the length of the file tracked by the sparse file tracker
     * @param ranges      the set of ranges to be considered present
     */
    public SparseFileTracker(String description, long length, SortedSet<ByteRange> ranges) {
        this.description = description;
        this.length = length;
        if (length < 0) {
            throw new IllegalArgumentException("Length [" + length + "] must be equal to or greater than 0 for [" + description + "]");
        }
        long initialLength = 0;
        if (ranges.isEmpty() == false) {
            synchronized (mutex) {
                Range previous = null;
                for (ByteRange next : ranges) {
                    if (next.length() == 0L) {
                        throw new IllegalArgumentException("Range " + next + " cannot be empty");
                    }
                    if (length < next.end()) {
                        throw new IllegalArgumentException("Range " + next + " is exceeding maximum length [" + length + ']');
                    }
                    final Range range = new Range(next);
                    if (previous != null && range.start <= previous.end) {
                        throw new IllegalArgumentException("Range " + range + " is overlapping a previous range " + previous);
                    }
                    final boolean added = this.ranges.add(range);
                    assert added : range + " already exist in " + this.ranges;
                    previous = range;
                    initialLength += range.end - range.start;
                }
                assert invariant();
            }
        }
        this.initialLength = initialLength;
    }

    public long getLength() {
        return length;
    }

    public SortedSet<ByteRange> getCompletedRanges() {
        SortedSet<ByteRange> completedRanges = null;
        synchronized (mutex) {
            assert invariant();
            for (Range range : ranges) {
                if (range.isPending()) {
                    continue;
                }
                if (completedRanges == null) {
                    completedRanges = new TreeSet<>();
                }
                completedRanges.add(ByteRange.of(range.start, range.end));
            }
        }
        return completedRanges == null ? Collections.emptySortedSet() : completedRanges;
    }

    /**
     * Returns the number of bytes that were initially present in the case where the sparse file tracker was initialized with some
     * completed ranges.
     * See {@link #SparseFileTracker(String, long, SortedSet)}
     */
    public long getInitialLength() {
        return initialLength;
    }

    /**
     * @return the sum of the length of the ranges
     */
    private long computeLengthOfRanges() {
        assert Thread.holdsLock(mutex) : "sum of length of the ranges must be computed under mutex";
        return ranges.stream().mapToLong(range -> range.end - range.start).sum();
    }

    /**
     * Called before reading a range from the file to ensure that this range is present. Returns a list of gaps for the caller to fill. The
     * range from the file is defined by {@code range} but the listener is executed as soon as a (potentially smaller) sub range
     * {@code subRange} becomes available.
     *
     * @param range    A ByteRange that contains the (inclusive) start and (exclusive) end of the desired range
     * @param subRange A ByteRange that contains the (inclusive) start and (exclusive) end of the listener's range
     * @param listener Listener for when the listening range is fully available
     * @return A collection of gaps that the client should fill in to satisfy this range
     * @throws IllegalArgumentException if invalid range is requested
     */
    public List<Gap> waitForRange(final ByteRange range, final ByteRange subRange, final ActionListener<Void> listener) {
        if (length < range.end()) {
            throw new IllegalArgumentException("invalid range [" + range + ", length=" + length + "]");
        }

        if (length < subRange.end()) {
            throw new IllegalArgumentException("invalid range to listen to [" + subRange + ", length=" + length + "]");
        }
        if (subRange.isSubRangeOf(range) == false) {
            throw new IllegalArgumentException(
                "unable to listen to range [start="
                    + subRange.start()
                    + ", end="
                    + subRange.end()
                    + "] when range is [start="
                    + range.start()
                    + ", end="
                    + range.end()
                    + ", length="
                    + length
                    + "]"
            );
        }

        if (complete >= range.end()) {
            listener.onResponse(null);
            return List.of();
        }
        return doWaitForRange(range, subRange, listener);
    }

    private List<Gap> doWaitForRange(ByteRange range, ByteRange subRange, ActionListener<Void> listener) {
        final ActionListener<Void> wrappedListener = wrapWithAssertions(listener);

        final List<Gap> gaps = new ArrayList<>();
        final List<Range> pendingRanges = new ArrayList<>();
        final Range targetRange = new Range(range);
        synchronized (mutex) {
            determineStartingRange(range, pendingRanges, targetRange);

            while (targetRange.start < range.end()) {
                assert 0 <= targetRange.start : targetRange;
                assert invariant();

                final Range firstExistingRange = ranges.ceiling(targetRange);
                if (firstExistingRange == null) {
                    final Range newPendingRange = new Range(
                        targetRange.start,
                        range.end(),
                        new ProgressListenableActionFuture(targetRange.start, range.end())
                    );
                    ranges.add(newPendingRange);
                    pendingRanges.add(newPendingRange);
                    gaps.add(new Gap(newPendingRange));
                    targetRange.start = range.end();
                } else {
                    assert targetRange.start <= firstExistingRange.start : targetRange + " vs " + firstExistingRange;

                    if (targetRange.start == firstExistingRange.start) {
                        if (firstExistingRange.isPending()) {
                            pendingRanges.add(firstExistingRange);
                        }
                        targetRange.start = Math.min(range.end(), firstExistingRange.end);
                    } else {
                        final long newPendingRangeEnd = Math.min(range.end(), firstExistingRange.start);
                        final Range newPendingRange = new Range(
                            targetRange.start,
                            newPendingRangeEnd,
                            new ProgressListenableActionFuture(targetRange.start, newPendingRangeEnd)
                        );
                        ranges.add(newPendingRange);
                        pendingRanges.add(newPendingRange);
                        gaps.add(new Gap(newPendingRange));
                        targetRange.start = newPendingRange.end;
                    }
                }
            }
            assert targetRange.start == targetRange.end : targetRange;
            assert targetRange.start == range.end() : targetRange;
            assert invariant();

            assert ranges.containsAll(pendingRanges) : ranges + " vs " + pendingRanges;
            assert pendingRanges.stream().allMatch(Range::isPending) : pendingRanges;
            assert pendingRanges.size() != 1 || gaps.size() <= 1 : gaps;
        }

        // Pending ranges that needs to be filled before executing the listener
        if (range.equals(subRange) == false) {
            pendingRanges.removeIf(pendingRange -> (pendingRange.start < subRange.end() && subRange.start() < pendingRange.end) == false);
            pendingRanges.sort(RANGE_START_COMPARATOR);
        }

        subscribeToCompletionListeners(pendingRanges, subRange.end(), wrappedListener);

        return Collections.unmodifiableList(gaps);
    }

    private void determineStartingRange(ByteRange range, List<Range> pendingRanges, Range targetRange) {
        assert invariant();
        final Range lastEarlierRange = ranges.lower(targetRange);
        if (lastEarlierRange != null) {
            if (range.start() < lastEarlierRange.end) {
                if (lastEarlierRange.isPending()) {
                    pendingRanges.add(lastEarlierRange);
                }
                targetRange.start = Math.min(range.end(), lastEarlierRange.end);
            }
        }
    }

    /**
     * Called before reading a range from the file to ensure that this range is present. Unlike
     * {@link SparseFileTracker#waitForRange(ByteRange, ByteRange, ActionListener)} this method does not expect the caller to fill in any
     * gaps.
     *
     * @param range    A tuple that contains the (inclusive) start and (exclusive) end of the desired range
     * @param listener Listener for when the listening range is fully available
     * @return {@code true} if the requested range is entirely pending or present and the listener will eventually be notified when the
     *                      range is entirely present; {@code false} if the requested range contains gaps that are not currently being
     *                      filled.
     * @throws IllegalArgumentException if invalid range is requested
     */
    public boolean waitForRangeIfPending(final ByteRange range, final ActionListener<Void> listener) {
        if (length < range.end()) {
            throw new IllegalArgumentException("invalid range [" + range + ", length=" + length + "]");
        }

        final ActionListener<Void> wrappedListener = wrapWithAssertions(listener);
        final List<Range> pendingRanges = new ArrayList<>();

        final Range targetRange = new Range(range);
        synchronized (mutex) {
            determineStartingRange(range, pendingRanges, targetRange);

            while (targetRange.start < range.end()) {
                assert 0 <= targetRange.start : targetRange;
                assert invariant();

                final Range firstExistingRange = ranges.ceiling(targetRange);
                if (firstExistingRange == null) {
                    return false;
                } else {
                    assert targetRange.start <= firstExistingRange.start : targetRange + " vs " + firstExistingRange;

                    if (targetRange.start == firstExistingRange.start) {
                        if (firstExistingRange.isPending()) {
                            pendingRanges.add(firstExistingRange);
                        }
                        targetRange.start = Math.min(range.end(), firstExistingRange.end);
                    } else {
                        return false;
                    }
                }
            }
            assert targetRange.start == targetRange.end : targetRange;
            assert targetRange.start == range.end() : targetRange;
            assert invariant();
        }

        subscribeToCompletionListeners(pendingRanges, range.end(), wrappedListener);
        return true;
    }

    private void subscribeToCompletionListeners(List<Range> requiredRanges, long rangeEnd, ActionListener<Void> listener) {
        // NB we work with ranges outside the mutex here, but only to interact with their completion listeners which are `final` so
        // there is no risk of concurrent modification.
        switch (requiredRanges.size()) {
            case 0 ->
                // no need to wait for the gaps to be filled, the listener can be executed immediately
                listener.onResponse(null);
            case 1 -> {
                final Range requiredRange = requiredRanges.get(0);
                requiredRange.completionListener.addListener(
                    listener.map(progress -> null),
                    Math.min(requiredRange.completionListener.end, rangeEnd)
                );
            }
            default -> {
                try (var listeners = new RefCountingListener(listener)) {
                    for (Range range : requiredRanges) {
                        range.completionListener.addListener(listeners.acquire(l -> {}), Math.min(range.completionListener.end, rangeEnd));
                    }
                }
            }
        }
    }

    private ActionListener<Void> wrapWithAssertions(ActionListener<Void> listener) {
        if (Assertions.ENABLED) {
            return ActionListener.runAfter(
                listener,
                () -> { assert Thread.holdsLock(mutex) == false : "mutex unexpectedly held in listener"; }
            );
        } else {
            return listener;
        }
    }

    /**
     * Returns a range that contains all bytes of the target range which are absent (possibly pending). The returned range may include
     * some ranges of present bytes. It tries to return the smallest possible range, but does so on a best-effort basis. This method does
     * not acquire anything, which means that another thread may concurrently fill in some of the returned range.
     *
     * @param range The target range
     * @return a range that contains all bytes of the target range which are absent, or {@code null} if there are no such bytes.
     */
    @Nullable
    public ByteRange getAbsentRangeWithin(ByteRange range) {
        synchronized (mutex) {

            final long start = range.start();
            // Find the first absent byte in the range
            final Range lastStartRange = ranges.floor(new Range(start, start, null));
            long resultStart;
            if (lastStartRange == null) {
                resultStart = start;
            } else {
                // last range which starts <= 'start' and which therefore may contain the first byte of the range
                if (lastStartRange.end < start) {
                    resultStart = start;
                } else if (lastStartRange.isPending()) {
                    resultStart = start;
                } else {
                    resultStart = lastStartRange.end;
                }
            }
            assert resultStart >= start;

            final long end = range.end();
            // Find the last absent byte in the range
            final Range lastEndRange = ranges.lower(new Range(end, end, null));

            final long resultEnd;
            if (lastEndRange == null) {
                resultEnd = end;
            } else {
                // last range which starts < 'end' and which therefore may contain the last byte of the range
                if (lastEndRange.end < end) {
                    resultEnd = end;
                } else if (lastEndRange.isPending()) {
                    resultEnd = end;
                } else {
                    resultEnd = lastEndRange.start;
                }
            }
            assert resultEnd <= end;

            return resultStart < resultEnd ? ByteRange.of(resultStart, resultEnd) : null;
        }
    }

    private boolean assertPendingRangeExists(Range range) {
        assert Thread.holdsLock(mutex);
        final SortedSet<Range> existingRanges = ranges.tailSet(range);
        assert existingRanges.isEmpty() == false;
        final Range existingRange = existingRanges.first();
        assert existingRange == range;
        assert existingRange.isPending();
        return true;
    }

    private void onGapSuccess(final Range gapRange) {
        final ProgressListenableActionFuture completionListener;

        synchronized (mutex) {
            assert invariant();
            assert assertPendingRangeExists(gapRange);
            completionListener = gapRange.completionListener;
            ranges.remove(gapRange);

            final SortedSet<Range> prevRanges = ranges.headSet(gapRange);
            final Range prevRange = prevRanges.isEmpty() ? null : prevRanges.last();
            assert prevRange == null || prevRange.end <= gapRange.start : prevRange + " vs " + gapRange;
            final boolean mergeWithPrev = prevRange != null && prevRange.isPending() == false && prevRange.end == gapRange.start;

            final SortedSet<Range> nextRanges = ranges.tailSet(gapRange);
            final Range nextRange = nextRanges.isEmpty() ? null : nextRanges.first();
            assert nextRange == null || gapRange.end <= nextRange.start : gapRange + " vs " + nextRange;
            final boolean mergeWithNext = nextRange != null && nextRange.isPending() == false && gapRange.end == nextRange.start;

            if (mergeWithPrev && mergeWithNext) {
                assert prevRange.isPending() == false : prevRange;
                assert nextRange.isPending() == false : nextRange;
                assert prevRange.end == gapRange.start : prevRange + " vs " + gapRange;
                assert gapRange.end == nextRange.start : gapRange + " vs " + nextRange;
                prevRange.end = nextRange.end;
                ranges.remove(nextRange);
                maybeUpdateCompletePointer(prevRange);
            } else if (mergeWithPrev) {
                assert prevRange.isPending() == false : prevRange;
                assert prevRange.end == gapRange.start : prevRange + " vs " + gapRange;
                prevRange.end = gapRange.end;
                maybeUpdateCompletePointer(prevRange);
            } else if (mergeWithNext) {
                assert nextRange.isPending() == false : nextRange;
                assert gapRange.end == nextRange.start : gapRange + " vs " + nextRange;
                nextRange.start = gapRange.start;
                maybeUpdateCompletePointer(nextRange);
            } else {
                maybeUpdateCompletePointer(gapRange);
                ranges.add(new Range(gapRange.start, gapRange.end, null));
            }

            assert invariant();
        }

        completionListener.onResponse(gapRange.end);
    }

    private void maybeUpdateCompletePointer(Range gapRange) {
        assert Thread.holdsLock(mutex);
        if (gapRange.start == 0) {
            complete = gapRange.end;
        }
    }

    private void onGapProgress(final Range gapRange, long value) {
        final ProgressListenableActionFuture completionListener;

        synchronized (mutex) {
            assert invariant();
            assert assertPendingRangeExists(gapRange);
            completionListener = gapRange.completionListener;
            assert invariant();
        }

        completionListener.onProgress(value);
    }

    private void onGapFailure(final Range gapRange, Exception e) {
        final ProgressListenableActionFuture completionListener;

        synchronized (mutex) {
            assert invariant();
            assert assertPendingRangeExists(gapRange);
            completionListener = gapRange.completionListener;
            final boolean removed = ranges.remove(gapRange);
            assert removed : gapRange + " not found";
            assert invariant();
        }

        completionListener.onFailure(e);
    }

    private boolean invariant() {
        assert Thread.holdsLock(mutex);
        long lengthOfRanges = 0L;
        Range previousRange = null;
        for (final Range range : ranges) {
            if (previousRange != null) {
                // ranges are nonempty
                assert range.start < range.end : range;

                // ranges are disjoint
                assert previousRange.end <= range.start : previousRange + " vs " + range;

                // contiguous, non-pending ranges are merged together
                assert previousRange.isPending() || range.isPending() || previousRange.end < range.start : previousRange + " vs " + range;

            }

            // range never exceed maximum length
            assert range.end <= length;

            lengthOfRanges += range.end - range.start;
            previousRange = range;
        }

        // sum of ranges lengths never exceed maximum length
        assert computeLengthOfRanges() <= length;

        // computed length of ranges is equal to the sum of range lengths
        assert computeLengthOfRanges() == lengthOfRanges;

        return true;
    }

    @Override
    public String toString() {
        return "SparseFileTracker[" + description + ']';
    }

    /**
     * Represents a gap in the file that a client should fill in.
     */
    public class Gap {

        /**
         * Range in the file corresponding to the current gap
         */
        public final Range range;

        Gap(Range range) {
            assert range.start < range.end : range.start + "-" + range.end;
            this.range = range;
        }

        public long start() {
            return range.start;
        }

        public long end() {
            return range.end;
        }

        public void onCompletion() {
            onGapSuccess(range);
        }

        public void onProgress(long value) {
            onGapProgress(range, value);
        }

        public void onFailure(Exception e) {
            onGapFailure(range, e);
        }

        @Override
        public String toString() {
            return SparseFileTracker.this.toString() + ' ' + range;
        }
    }

    private static class Range {
        /**
         * Inclusive start point of this range
         */
        long start;

        /**
         * Exclusive end point of this range
         */
        long end;

        @Nullable // if not pending
        final ProgressListenableActionFuture completionListener;

        Range(ByteRange range) {
            this(range.start(), range.end(), null);
        }

        Range(long start, long end, @Nullable ProgressListenableActionFuture completionListener) {
            assert start <= end : start + "-" + end;
            this.start = start;
            this.end = end;
            this.completionListener = completionListener;
        }

        boolean isPending() {
            return completionListener != null;
        }

        @Override
        public String toString() {
            return "[" + start + "-" + end + (isPending() ? ", pending]" : "]");
        }
    }
}
