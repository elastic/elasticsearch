/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.util.stream.Stream;

public class LeakTrackerTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger();

    private final TrackedObjectTestCase trackedObjectTestCase;
    private ReachabilityChecker reachabilityChecker;

    @Before
    public void createReachabilityTracker() {
        reachabilityChecker = new ReachabilityChecker();
    }

    public LeakTrackerTests(@Name("trackingMethod") TrackedObjectTestCase trackedObjectTestCase) {
        this.trackedObjectTestCase = trackedObjectTestCase;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return Stream.of(
            new PojoTrackedObjectTestCase(),
            new ReleasableTrackedObjectTestCase(),
            new ReferenceCountedTrackedObjectTestCase()
        ).map(i -> new Object[] { i }).toList();
    }

    public void testWillLogErrorWhenTrackedObjectIsNotClosed() throws Exception {
        trackedObjectTestCase.makeAssumptions();
        // Let it go out of scope without closing
        trackedObjectTestCase.createAndTrack(reachabilityChecker);
        reachabilityChecker.ensureUnreachable();
        assertBusy(() -> assertLeakDetected("LEAK: resource was not cleaned up before it was garbage-collected\\.(.*|\\s)*"));
    }

    public void testWillNotLogErrorWhenTrackedObjectIsClosed() throws IOException {
        trackedObjectTestCase.makeAssumptions();
        // Close before letting it go out of scope
        trackedObjectTestCase.createAndTrack(reachabilityChecker).close();
        reachabilityChecker.ensureUnreachable();
    }

    /**
     * Encapsulates the lifecycle for a particular type of tracked object
     */
    public interface TrackedObjectTestCase {

        default void makeAssumptions() {};

        /**
         * Create the tracked object, implementations must
         * - track it with the {@link LeakTracker}
         * - register it with the passed reachability checker
         * @param reachabilityChecker The reachability checker
         * @return A {@link Closeable} that retains a reference to the tracked object, and when closed will do the appropriate cleanup
         */
        Closeable createAndTrack(ReachabilityChecker reachabilityChecker);
    }

    private static class PojoTrackedObjectTestCase implements TrackedObjectTestCase {

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

    private static class ReferenceCountedTrackedObjectTestCase implements TrackedObjectTestCase {

        @Override
        public void makeAssumptions() {
            assumeTrue("ReferenceCounted wrapper is a no-op when assertions are disabled", Assertions.ENABLED);
        }

        @Override
        public Closeable createAndTrack(ReachabilityChecker reachabilityChecker) {
            RefCounted refCounted = LeakTracker.wrap(reachabilityChecker.register(createRefCounted()));
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

        private static RefCounted createRefCounted() {
            return new AbstractRefCounted() {

                @Override
                protected void closeInternal() {
                    // Do nothing
                }
            };
        }
    }

    private static class ReleasableTrackedObjectTestCase implements TrackedObjectTestCase {

        @Override
        public void makeAssumptions() {
            assumeTrue("Releasable wrapper is a no-op when assertions are disabled", Assertions.ENABLED);
        }

        @Override
        public Closeable createAndTrack(ReachabilityChecker reachabilityChecker) {
            return LeakTracker.wrap(reachabilityChecker.register(createReleasable()));
        }

        @Override
        public String toString() {
            return "LeakTracker.wrap(Releasable)";
        }

        private static Releasable createReleasable() {
            int number = Randomness.get().nextInt();
            return () -> logger.info("Prevents this returning a non-collectible constant {}", number);
        }
    }
}
