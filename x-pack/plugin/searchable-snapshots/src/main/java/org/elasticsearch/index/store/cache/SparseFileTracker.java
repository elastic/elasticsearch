/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Keeps track of the contents of a file that may not be completely present.
 */
public class SparseFileTracker {

    /**
     * The byte ranges of the file which are present or pending. These ranges are nonempty, disjoint (and in order) and the non-pending
     * ranges are not contiguous (i.e. contiguous non-pending ranges are merged together). See {@link SparseFileTracker#invariant()} for
     * details.
     */
    private final TreeSet<Range> ranges = new TreeSet<>(Comparator.comparingLong(r -> r.start));

    private final Object mutex = new Object();

    private final String description;

    private final long length;

    public SparseFileTracker(String description, long length) {
        this.description = description;
        this.length = length;
        if (length < 0) {
            throw new IllegalArgumentException("Length [" + length + "] must be equal to or greater than 0 for [" + description + "]");
        }
    }

    public long getLength() {
        return length;
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
     * @param range    A tuple that contains the (inclusive) start and (exclusive) end of the desired range
     * @param subRange A tuple that contains the (inclusive) start and (exclusive) end of the listener's range
     * @param listener Listener for when the listening range is fully available
     * @return A collection of gaps that the client should fill in to satisfy this range
     * @throws IllegalArgumentException if invalid range is requested
     */
    public List<Gap> waitForRange(final Tuple<Long, Long> range, final Tuple<Long, Long> subRange, final ActionListener<Void> listener) {
        final long start = range.v1();
        final long end = range.v2();
        if (end < start || start < 0L || length < end) {
            throw new IllegalArgumentException("invalid range [start=" + start + ", end=" + end + ", length=" + length + "]");
        }

        if (subRange.v2() < subRange.v1() || subRange.v1() < 0L || length < subRange.v2()) {
            throw new IllegalArgumentException(
                "invalid range to listen to [start=" + subRange.v1() + ", end=" + subRange.v2() + ", length=" + length + "]"
            );
        }
        if (subRange.v1() < start || end < subRange.v2()) {
            throw new IllegalArgumentException(
                "unable to listen to range [start="
                    + subRange.v1()
                    + ", end="
                    + subRange.v2()
                    + "] when range is [start="
                    + start
                    + ", end="
                    + end
                    + ", length="
                    + length
                    + "]"
            );
        }

        final List<Gap> gaps = new ArrayList<>();
        synchronized (mutex) {
            assert invariant();

            final List<Range> pendingRanges = new ArrayList<>();

            final Range targetRange = new Range(start, end, null);
            final SortedSet<Range> earlierRanges = ranges.headSet(targetRange, false); // ranges with strictly earlier starts
            if (earlierRanges.isEmpty() == false) {
                final Range lastEarlierRange = earlierRanges.last();
                if (start < lastEarlierRange.end) {
                    if (lastEarlierRange.isPending()) {
                        pendingRanges.add(lastEarlierRange);
                    }
                    targetRange.start = Math.min(end, lastEarlierRange.end);
                }
            }

            while (targetRange.start < end) {
                assert 0 <= targetRange.start : targetRange;
                assert invariant();

                final SortedSet<Range> existingRanges = ranges.tailSet(targetRange);
                if (existingRanges.isEmpty()) {
                    final Range newPendingRange = new Range(
                        targetRange.start,
                        end,
                        new ProgressListenableActionFuture(targetRange.start, end)
                    );
                    ranges.add(newPendingRange);
                    pendingRanges.add(newPendingRange);
                    gaps.add(new Gap(newPendingRange));
                    targetRange.start = end;
                } else {
                    final Range firstExistingRange = existingRanges.first();
                    assert targetRange.start <= firstExistingRange.start : targetRange + " vs " + firstExistingRange;

                    if (targetRange.start == firstExistingRange.start) {
                        if (firstExistingRange.isPending()) {
                            pendingRanges.add(firstExistingRange);
                        }
                        targetRange.start = Math.min(end, firstExistingRange.end);
                    } else {
                        final long newPendingRangeEnd = Math.min(end, firstExistingRange.start);
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
            assert targetRange.start == end : targetRange;
            assert invariant();

            if (pendingRanges.isEmpty() == false) {
                assert ranges.containsAll(pendingRanges) : ranges + " vs " + pendingRanges;
                assert pendingRanges.stream().allMatch(Range::isPending) : pendingRanges;
                assert pendingRanges.size() != 1 || gaps.size() <= 1 : gaps;

                // Pending ranges that needs to be filled before executing the listener
                final List<Range> requiredRanges = (start == subRange.v1() && end == subRange.v2())
                    ? pendingRanges
                    : pendingRanges.stream()
                        .filter(pendingRange -> pendingRange.start < subRange.v2())
                        .filter(pendingRange -> subRange.v1() < pendingRange.end)
                        .sorted(Comparator.comparingLong(r -> r.start))
                        .collect(Collectors.toList());

                switch (requiredRanges.size()) {
                    case 0:
                        // no need to wait for the gaps to be filled, the listener can be executed immediately
                        listener.onResponse(null);
                        break;
                    case 1:
                        final Range requiredRange = requiredRanges.get(0);
                        requiredRange.completionListener.addListener(
                            ActionListener.map(listener, progress -> null),
                            Math.min(requiredRange.end, subRange != null ? subRange.v2() : Long.MAX_VALUE)
                        );
                        break;
                    default:
                        final GroupedActionListener<Long> groupedActionListener = new GroupedActionListener<>(
                            ActionListener.map(listener, progress -> null),
                            requiredRanges.size()
                        );
                        requiredRanges.forEach(
                            r -> r.completionListener.addListener(
                                groupedActionListener,
                                Math.min(r.end, subRange != null ? subRange.v2() : Long.MAX_VALUE)
                            )
                        );
                }

                return Collections.unmodifiableList(gaps);
            }
        }

        assert gaps.isEmpty(); // or else pendingRanges.isEmpty() == false so we already returned
        listener.onResponse(null);
        return Collections.emptyList();
    }

    /**
     * Returns a range that contains all bytes of the target range which are absent (possibly pending). The returned range may include
     * some ranges of present bytes. It tries to return the smallest possible range, but does so on a best-effort basis. This method does
     * not acquire anything, which means that another thread may concurrently fill in some of the returned range.
     *
     * @param start The (inclusive) start of the target range
     * @param end The (exclusive) end of the target range
     * @return a range that contains all bytes of the target range which are absent, or {@code null} if there are no such bytes.
     */
    public Tuple<Long, Long> getAbsentRangeWithin(final long start, final long end) {
        synchronized (mutex) {

            // Find the first absent byte in the range
            final SortedSet<Range> startRanges = ranges.headSet(new Range(start, start, null), true); // ranges which start <= 'start'
            long resultStart;
            if (startRanges.isEmpty()) {
                resultStart = start;
            } else {
                final Range lastStartRange = startRanges.last();
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

            // Find the last absent byte in the range
            final SortedSet<Range> endRanges = ranges.headSet(new Range(end, end, null), false); // ranges which start < 'end'
            final long resultEnd;
            if (endRanges.isEmpty()) {
                resultEnd = end;
            } else {
                final Range lastEndRange = endRanges.last();
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

            return resultStart < resultEnd ? Tuple.tuple(resultStart, resultEnd) : null;
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
            } else if (mergeWithPrev) {
                assert prevRange.isPending() == false : prevRange;
                assert prevRange.end == gapRange.start : prevRange + " vs " + gapRange;
                prevRange.end = gapRange.end;
            } else if (mergeWithNext) {
                assert nextRange.isPending() == false : nextRange;
                assert gapRange.end == nextRange.start : gapRange + " vs " + nextRange;
                nextRange.start = gapRange.start;
            } else {
                ranges.add(new Range(gapRange.start, gapRange.end, null));
            }

            assert invariant();
        }

        completionListener.onResponse(gapRange.end);
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
