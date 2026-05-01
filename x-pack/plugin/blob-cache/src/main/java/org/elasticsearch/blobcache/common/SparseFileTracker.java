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
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.LongConsumer;

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
        this.initialLength = ranges.isEmpty() ? 0 : addInitialRanges(length, ranges);
    }

    private long addInitialRanges(long length, SortedSet<ByteRange> ranges) {
        long initialLength = 0;
        synchronized (this.ranges) {
            Range previous = null;
            for (ByteRange next : ranges) {
                if (next.isEmpty()) {
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
        return initialLength;
    }

    public long getLength() {
        return length;
    }

    public SortedSet<ByteRange> getCompletedRanges() {
        SortedSet<ByteRange> completedRanges = null;
        synchronized (ranges) {
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
        assert Thread.holdsLock(ranges) : "sum of length of the ranges must be computed under mutex";
        return ranges.stream().mapToLong(range -> range.end - range.start).sum();
    }

    /**
     * Called before reading a range from the file to ensure that this range is present. Returns a {@link Gaps} for the caller to claim
     * and fill, unless there are no gaps at call time in which case the optional is empty. The range from the file is defined by
     * {@code range} but the listener is executed as soon as a (potentially smaller) sub range {@code subRange} becomes available.
     * Notice that the gaps returned may extend beyond the `range` and then the caller is still responsible for filling them.
     *
     * @param range    A ByteRange that contains the (inclusive) start and (exclusive) end of the desired range
     * @param subRange A ByteRange that contains the (inclusive) start and (exclusive) end of the listener's range
     * @param listener Listener for when the listening range is fully available
     * @return an Optional containing the gaps to fill, or empty if there are no gaps to fill
     * @throws IllegalArgumentException if invalid range is requested
     */
    public Optional<Gaps> waitForRange(final ByteRange range, final ByteRange subRange, final ActionListener<Void> listener) {
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

        if (subRange.end() <= complete) {
            listener.onResponse(null);
            return Optional.empty();
        }
        return doWaitForRange(range, subRange, listener);
    }

    private Optional<Gaps> doWaitForRange(ByteRange range, ByteRange subRange, ActionListener<Void> listener) {
        final ActionListener<Void> wrappedListener = wrapWithAssertions(listener);

        final List<Range> pendingRanges = new ArrayList<>();
        final Range targetRange = new Range(range);
        boolean hasGaps;
        synchronized (ranges) {
            hasGaps = determineStartingRange(range, pendingRanges, targetRange);

            while (targetRange.start < range.end()) {
                assert 0 <= targetRange.start : targetRange;
                assert invariant();

                final Range firstExistingRange = ranges.ceiling(targetRange);
                if (firstExistingRange == null) {
                    final Range newPendingRange = new Range(
                        targetRange.start,
                        range.end(),
                        new ProgressListenableActionFuture(targetRange.start, range.end(), progressConsumer(targetRange.start))
                    );
                    ranges.add(newPendingRange);
                    pendingRanges.add(newPendingRange);
                    hasGaps = true;
                    targetRange.start = range.end();
                } else {
                    assert targetRange.start <= firstExistingRange.start : targetRange + " vs " + firstExistingRange;

                    if (targetRange.start == firstExistingRange.start) {
                        if (firstExistingRange.isPending()) {
                            pendingRanges.add(firstExistingRange);
                            if (firstExistingRange.claimed == false) {
                                hasGaps = true;
                            }
                        }
                        targetRange.start = Math.min(range.end(), firstExistingRange.end);
                    } else {
                        final long newPendingRangeEnd = Math.min(range.end(), firstExistingRange.start);
                        final Range newPendingRange = new Range(
                            targetRange.start,
                            newPendingRangeEnd,
                            new ProgressListenableActionFuture(targetRange.start, newPendingRangeEnd, progressConsumer(targetRange.start))
                        );
                        ranges.add(newPendingRange);
                        pendingRanges.add(newPendingRange);
                        hasGaps = true;
                        targetRange.start = newPendingRange.end;
                    }
                }
            }
            assert targetRange.start == targetRange.end : targetRange;
            assert targetRange.start == range.end() : targetRange;
            assert invariant();

            assert ranges.containsAll(pendingRanges) : ranges + " vs " + pendingRanges;
            assert pendingRanges.stream().allMatch(Range::isPending) : pendingRanges;
        }

        // Pending ranges that needs to be filled before executing the listener
        if (range.equals(subRange) == false) {
            pendingRanges.removeIf(pendingRange -> (pendingRange.start < subRange.end() && subRange.start() < pendingRange.end) == false);
            pendingRanges.sort(RANGE_START_COMPARATOR);
        }

        subscribeToCompletionListeners(pendingRanges, subRange.end(), wrappedListener);

        return hasGaps ? Optional.of(new Gaps(range)) : Optional.empty();
    }

    /**
     * Populate `pendingRanges` with a prior range overlapping into `range` (if pending), update targetRange.start to allow
     * searching for further ranges. Return whether an unclaimed range was added to pendingRanges, i.e., a gap still needs
     * an owner.
     * @param range the range that is to be filled, where we want to find the first existing range for.
     * @param pendingRanges the pendingRanges to add a range that overlaps into the range we want to fill.
     * @param targetRange the targetRange to populate the start value of.
     * @return whether a gap still need to be claimed.
     */
    private boolean determineStartingRange(ByteRange range, List<Range> pendingRanges, Range targetRange) {
        assert invariant();
        final Range lastEarlierRange = ranges.lower(targetRange);
        if (lastEarlierRange != null) {
            if (range.start() < lastEarlierRange.end) {
                boolean unclaimed = false;
                if (lastEarlierRange.isPending()) {
                    pendingRanges.add(lastEarlierRange);
                    unclaimed = lastEarlierRange.claimed == false;
                }
                targetRange.start = Math.min(range.end(), lastEarlierRange.end);
                return unclaimed;
            }
        }
        return false;
    }

    private LongConsumer progressConsumer(long rangeStart) {
        assert Thread.holdsLock(ranges);
        if (rangeStart == complete) {
            return this::updateCompletePointer;
        } else {
            return null;
        }
    }

    public boolean checkAvailable(long upTo) {
        assert upTo <= length : "tried to check availability up to [" + upTo + "] but length is only [" + length + "]";
        return complete >= upTo;
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
        synchronized (ranges) {
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

    private static void subscribeToCompletionListeners(List<Range> requiredRanges, long rangeEnd, ActionListener<Void> listener) {
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
                () -> { assert Thread.holdsLock(ranges) == false : "mutex unexpectedly held in listener"; }
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
        synchronized (ranges) {

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

    /**
     * Returns the number of bytes of the target range which are absent (possibly pending). This method does
     * not acquire anything, which means that another thread may concurrently fill in some of the returned bytes.
     *
     * @param range The target range
     * @return the number of bytes of the target range which are or were absent
     */
    public long getAbsentBytesWithin(ByteRange range) {
        final long start = range.start();
        final long end = range.end();
        synchronized (ranges) {
            if (ranges.isEmpty()) {
                return end - start;
            } else {
                // Find the first absent byte in the range
                final Range startRange = new Range(start, start, null);
                final Range lastStartRange = ranges.floor(startRange);
                SortedSet<Range> subRange = ranges.subSet(
                    lastStartRange == null || lastStartRange.end <= start ? startRange : lastStartRange,
                    new Range(end, end, null)
                );
                long last = start;
                long sum = 0;
                for (Range r : subRange) {
                    sum += Math.max(r.start - last, 0);
                    if (r.isPending()) {
                        sum += Math.min(r.end, end) - Math.max(r.start, last);
                    }
                    last = r.end;
                }
                sum += Math.max(end - last, 0);
                return sum;
            }
        }
    }

    private boolean assertPendingRangeExists(Range range) {
        assert Thread.holdsLock(ranges);
        final SortedSet<Range> existingRanges = ranges.tailSet(range);
        assert existingRanges.isEmpty() == false;
        final Range existingRange = existingRanges.first();
        assert existingRange == range;
        assert existingRange.isPending();
        return true;
    }

    private void onGapSuccess(final Range gapRange) {
        synchronized (ranges) {
            assert invariant();
            assert assertPendingRangeExists(gapRange);
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

        gapRange.completionListener.onResponse(gapRange.end);
    }

    private void maybeUpdateCompletePointer(Range gapRange) {
        assert Thread.holdsLock(ranges);
        if (gapRange.start == 0) {
            updateCompletePointerHoldingLock(gapRange.end);
        }
    }

    private void updateCompletePointerHoldingLock(long value) {
        assert Thread.holdsLock(ranges);
        assert complete <= value : complete + ">" + value;
        complete = value;
    }

    private void updateCompletePointer(long value) {
        synchronized (ranges) {
            updateCompletePointerHoldingLock(value);
        }
    }

    // used in tests
    long getComplete() {
        return complete;
    }

    private boolean assertGapRangePending(Range gapRange) {
        synchronized (ranges) {
            assert invariant();
            assert assertPendingRangeExists(gapRange);
        }
        return true;
    }

    private void onGapFailure(final Range gapRange, Exception e) {
        synchronized (ranges) {
            assert invariant();
            assert assertPendingRangeExists(gapRange);
            final boolean removed = ranges.remove(gapRange);
            assert removed : gapRange + " not found";
            assert invariant();
        }

        gapRange.completionListener.onFailure(e);
    }

    private boolean invariant() {
        assert Thread.holdsLock(ranges);
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
        return "SparseFileTracker{description=" + description + ", length=" + length + ", complete=" + complete + '}';
    }

    /**
     * Represents a gap in the file that a client should fill in.
     */
    public class Gap {

        /**
         * Range in the file corresponding to the current gap
         */
        private final Range range;

        private Gap(Range range) {
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
            assert assertGapRangePending(range);
            range.completionListener.onProgress(value);
        }

        public void onFailure(Exception e) {
            onGapFailure(range, e);
        }

        @Override
        public String toString() {
            return SparseFileTracker.this.toString() + ' ' + range;
        }
    }

    /**
     * Represents the set of gaps returned by {@link #waitForRange} that a caller may claim to fill.
     */
    public class Gaps {

        private final ByteRange range;

        Gaps(ByteRange range) {
            this.range = range;
        }

        /**
         * Claims all unclaimed pending gaps within the original range, under the ranges lock so that
         * exactly one caller across concurrent claimers wins each gap. The caller is responsible for
         * filling the returned gaps.
         */
        public List<Gap> claim() {
            synchronized (SparseFileTracker.this.ranges) {
                List<Gap> claimed = null;
                final Range probe = new Range(range);
                Range current;
                final Range lowerRange = ranges.floor(probe);
                if (lowerRange != null && lowerRange.end > range.start()) {
                    current = lowerRange;
                } else {
                    current = ranges.ceiling(probe);
                }
                while (current != null && current.start < range.end()) {
                    if (current.isPending() && current.claimed == false) {
                        current.claimed = true;
                        if (claimed == null) {
                            claimed = new ArrayList<>();
                        }
                        claimed.add(new Gap(current));
                    }
                    current = ranges.higher(current);
                }
                return claimed == null ? List.of() : Collections.unmodifiableList(claimed);
            }
        }

        @Override
        public String toString() {
            return "Gaps{range=" + range + '}';
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

        // Tracks whether a caller has claimed this pending range for filling; only meaningful when isPending(), guarded by the ranges lock.
        boolean claimed;

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
