/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.util.stream.Stream;

public class LeakTrackerTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger();

    private final TrackedObjectLifecycle trackedObjectLifecycle;
    private ReachabilityChecker reachabilityChecker;

    @Before
    public void createReachabilityTracker() {
        reachabilityChecker = new ReachabilityChecker();
    }

    @Before
    public void onlyRunWhenAssertionsAreEnabled() {
        assumeTrue("Many of these tests don't make sense when assertions are disabled", Assertions.ENABLED);
    }

    public LeakTrackerTests(@Name("trackingMethod") TrackedObjectLifecycle trackedObjectLifecycle) {
        this.trackedObjectLifecycle = trackedObjectLifecycle;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return Stream.of(
            new PojoTrackedObjectLifecycle(),
            new ReleasableTrackedObjectLifecycle(),
            new ReferenceCountedTrackedObjectLifecycle()
        ).map(i -> new Object[] { i }).toList();
    }

    @SuppressWarnings("resource")
    public void testWillLogErrorWhenTrackedObjectIsNotClosed() throws Exception {
        // Let it go out of scope without closing
        trackedObjectLifecycle.createAndTrack(reachabilityChecker);
        reachabilityChecker.ensureUnreachable();
        assertBusy(ESTestCase::assertLeakDetected);
    }

    public void testWillNotLogErrorWhenTrackedObjectIsClosed() throws IOException {
        // Close before letting it go out of scope
        trackedObjectLifecycle.createAndTrack(reachabilityChecker).close();
        reachabilityChecker.ensureUnreachable();
    }

    /**
     * Encapsulates the lifecycle for a particular type of tracked object
     */
    public interface TrackedObjectLifecycle {

        /**
         * Create the tracked object, implementations must
         * - track it with the {@link LeakTracker}
         * - register it with the passed reachability checker
         * @param reachabilityChecker The reachability checker
         * @return A {@link Closeable} that retains a reference to the tracked object, and when closed will do the appropriate cleanup
         */
        Closeable createAndTrack(ReachabilityChecker reachabilityChecker);
    }

    private static class PojoTrackedObjectLifecycle implements TrackedObjectLifecycle {

        @Override
        public Closeable createAndTrack(ReachabilityChecker reachabilityChecker) {
            final Object object = reachabilityChecker.register(new Object());
            final LeakTracker.Leak leak = LeakTracker.INSTANCE.track(object);
            return () -> {
                logger.info("This log line retains a reference to {}", object);
                leak.close();
            };
        }

        @Override
        public String toString() {
            return "LeakTracker.INSTANCE.track(Object)";
        }
    }

    private static class ReferenceCountedTrackedObjectLifecycle implements TrackedObjectLifecycle {

        @Override
        public Closeable createAndTrack(ReachabilityChecker reachabilityChecker) {
            RefCounted refCounted = LeakTracker.wrap(reachabilityChecker.register((RefCounted) AbstractRefCounted.of(() -> {})));
            refCounted.incRef();
            refCounted.tryIncRef();
            return () -> {
                refCounted.decRef();    // tryIncRef
                refCounted.decRef();    // incRef
                refCounted.decRef();    // implicit
            };
        }

        @Override
        public String toString() {
            return "LeakTracker.wrap(RefCounted)";
        }
    }

    private static class ReleasableTrackedObjectLifecycle implements TrackedObjectLifecycle {

        @Override
        public Closeable createAndTrack(ReachabilityChecker reachabilityChecker) {
            return LeakTracker.wrap(reachabilityChecker.register(Releasables.assertOnce(() -> {})));
        }

        @Override
        public String toString() {
            return "LeakTracker.wrap(Releasable)";
        }
    }
}
