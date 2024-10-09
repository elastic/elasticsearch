/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common;

import org.elasticsearch.core.TimeValue;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a set of generic backoff policies. Backoff policies are used to calculate the number of times an action will be retried
 * and the intervals between those retries.
 *
 * Notes for implementing custom subclasses:
 *
 * The underlying mathematical principle of <code>BackoffPolicy</code> are progressions which can be either finite or infinite although
 * the latter should not be used for retrying. A progression can be mapped to a <code>java.util.Iterator</code> with the following
 * semantics:
 *
 * <ul>
 *     <li><code>#hasNext()</code> determines whether the progression has more elements. Return <code>true</code> for infinite progressions
 *     </li>
 *     <li><code>#next()</code> determines the next element in the progression, i.e. the next wait time period</li>
 * </ul>
 *
 * Note that backoff policies are exposed as <code>Iterables</code> in order to be consumed multiple times.
 */
public abstract class BackoffPolicy implements Iterable<TimeValue> {
    private static final BackoffPolicy NO_BACKOFF = new NoBackoff();

    /**
     * Creates a backoff policy that will not allow any backoff, i.e. an operation will fail after the first attempt.
     *
     * @return A backoff policy without any backoff period. The returned instance is thread safe.
     */
    public static BackoffPolicy noBackoff() {
        return NO_BACKOFF;
    }

    /**
     * Creates an new constant backoff policy with the provided configuration.
     *
     * @param delay              The delay defines how long to wait between retry attempts. Must not be null.
     *                           Must be &lt;= <code>Integer.MAX_VALUE</code> ms.
     * @param maxNumberOfRetries The maximum number of retries. Must be a non-negative number.
     * @return A backoff policy with a constant wait time between retries. The returned instance is thread safe but each
     * iterator created from it should only be used by a single thread.
     */
    public static BackoffPolicy constantBackoff(TimeValue delay, int maxNumberOfRetries) {
        return new ConstantBackoff(checkDelay(delay), maxNumberOfRetries);
    }

    /**
     * Creates an new exponential backoff policy with a default configuration of 50 ms initial wait period and 8 retries taking
     * roughly 5.1 seconds in total.
     *
     * @return A backoff policy with an exponential increase in wait time for retries. The returned instance is thread safe but each
     * iterator created from it should only be used by a single thread.
     */
    public static BackoffPolicy exponentialBackoff() {
        return exponentialBackoff(TimeValue.timeValueMillis(50), 8);
    }

    /**
     * Creates an new exponential backoff policy with the provided configuration.
     *
     * @param initialDelay       The initial delay defines how long to wait for the first retry attempt. Must not be null.
     *                           Must be &lt;= <code>Integer.MAX_VALUE</code> ms.
     * @param maxNumberOfRetries The maximum number of retries. Must be a non-negative number.
     * @return A backoff policy with an exponential increase in wait time for retries. The returned instance is thread safe but each
     * iterator created from it should only be used by a single thread.
     */
    public static BackoffPolicy exponentialBackoff(TimeValue initialDelay, int maxNumberOfRetries) {
        return new ExponentialBackoff((int) checkDelay(initialDelay).millis(), maxNumberOfRetries);
    }

    /**
     * Wraps the backoff policy in one that calls a method every time a new backoff is taken from the policy.
     */
    public static BackoffPolicy wrap(BackoffPolicy delegate, Runnable onBackoff) {
        return new WrappedBackoffPolicy(delegate, onBackoff);
    }

    private static TimeValue checkDelay(TimeValue delay) {
        if (delay.millis() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("delay must be <= " + Integer.MAX_VALUE + " ms");
        }
        return delay;
    }

    private static class NoBackoff extends BackoffPolicy {
        @Override
        public Iterator<TimeValue> iterator() {
            return Collections.emptyIterator();
        }
    }

    private static class ExponentialBackoff extends BackoffPolicy {
        private final int start;

        private final int numberOfElements;

        private ExponentialBackoff(int start, int numberOfElements) {
            assert start >= 0;
            assert numberOfElements >= 0;
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new ExponentialBackoffIterator(start, numberOfElements);
        }
    }

    private static class ExponentialBackoffIterator implements Iterator<TimeValue> {
        private final int numberOfElements;

        private final int start;

        private int currentlyConsumed;

        private ExponentialBackoffIterator(int start, int numberOfElements) {
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        @Override
        public boolean hasNext() {
            return currentlyConsumed < numberOfElements;
        }

        @Override
        public TimeValue next() {
            if (hasNext() == false) {
                throw new NoSuchElementException("Only up to " + numberOfElements + " elements");
            }
            int result = start + 10 * ((int) Math.exp(0.8d * (currentlyConsumed)) - 1);
            currentlyConsumed++;
            return TimeValue.timeValueMillis(result);
        }
    }

    private static final class ConstantBackoff extends BackoffPolicy {
        private final TimeValue delay;

        private final int numberOfElements;

        ConstantBackoff(TimeValue delay, int numberOfElements) {
            assert numberOfElements >= 0;
            this.delay = delay;
            this.numberOfElements = numberOfElements;
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new ConstantBackoffIterator(delay, numberOfElements);
        }
    }

    private static final class ConstantBackoffIterator implements Iterator<TimeValue> {
        private final TimeValue delay;
        private final int numberOfElements;
        private int curr;

        ConstantBackoffIterator(TimeValue delay, int numberOfElements) {
            this.delay = delay;
            this.numberOfElements = numberOfElements;
        }

        @Override
        public boolean hasNext() {
            return curr < numberOfElements;
        }

        @Override
        public TimeValue next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            curr++;
            return delay;
        }
    }

    private static final class WrappedBackoffPolicy extends BackoffPolicy {
        private final BackoffPolicy delegate;
        private final Runnable onBackoff;

        WrappedBackoffPolicy(BackoffPolicy delegate, Runnable onBackoff) {
            this.delegate = delegate;
            this.onBackoff = onBackoff;
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new WrappedBackoffIterator(delegate.iterator(), onBackoff);
        }
    }

    private static final class WrappedBackoffIterator implements Iterator<TimeValue> {
        private final Iterator<TimeValue> delegate;
        private final Runnable onBackoff;

        WrappedBackoffIterator(Iterator<TimeValue> delegate, Runnable onBackoff) {
            this.delegate = delegate;
            this.onBackoff = onBackoff;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public TimeValue next() {
            if (false == delegate.hasNext()) {
                throw new NoSuchElementException();
            }
            onBackoff.run();
            return delegate.next();
        }
    }
}
