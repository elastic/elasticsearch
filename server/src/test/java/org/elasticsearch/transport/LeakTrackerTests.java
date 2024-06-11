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
        trackedObjectTestCase.createAndTrack(reachabilityChecker);
        // Do not close leak before nullifying
        trackedObjectTestCase.nullifyReference();
        reachabilityChecker.ensureUnreachable();
        assertBusy(() -> assertLeakDetected("LEAK: resource was not cleaned up before it was garbage-collected\\.(.*|\\s)*"));
    }

    public void testWillNotLogErrorWhenTrackedObjectIsClosed() {
        trackedObjectTestCase.makeAssumptions();
        trackedObjectTestCase.createAndTrack(reachabilityChecker);
        trackedObjectTestCase.closeLeak();
        trackedObjectTestCase.nullifyReference();
        // Should not detect a leak
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
         * - retain a reference to it
         * @param reachabilityChecker The reachability checker
         */
        void createAndTrack(ReachabilityChecker reachabilityChecker);

        /**
         * Nullify the retained reference
         */
        void nullifyReference();

        /**
         * Close the {@link LeakTracker.Leak}
         */
        void closeLeak();
    }

    private static class PojoTrackedObjectTestCase implements TrackedObjectTestCase {

        private Object object;
        private LeakTracker.Leak leak;

        @Override
        public void createAndTrack(ReachabilityChecker reachabilityChecker) {
            object = reachabilityChecker.register(new Object());
            leak = LeakTracker.INSTANCE.track(object);
        }

        @Override
        public void nullifyReference() {
            object = null;
        }

        @Override
        public void closeLeak() {
            leak.close();
        }

        @Override
        public String toString() {
            return "LeakTracker.INSTANCE.track(Object)";
        }
    }

    private static class ReferenceCountedTrackedObjectTestCase implements TrackedObjectTestCase {

        private RefCounted refCounted;

        @Override
        public void makeAssumptions() {
            assumeTrue("ReferenceCounted wrapper is a no-op when assertions are disabled", Assertions.ENABLED);
        }

        @Override
        public void createAndTrack(ReachabilityChecker reachabilityChecker) {
            RefCounted refCounted = reachabilityChecker.register(createRefCounted());
            this.refCounted = LeakTracker.wrap(refCounted);
            this.refCounted.incRef();
            this.refCounted.tryIncRef();
        }

        @Override
        public void nullifyReference() {
            refCounted = null;
        }

        @Override
        public void closeLeak() {
            refCounted.decRef();    // tryIncRef
            refCounted.decRef();    // incRef
            refCounted.decRef();    // implicit
        }

        @Override
        public String toString() {
            return "LeakTracker.wrap(RefCounted)";
        }

        private static RefCounted createRefCounted() {
            int number = Randomness.get().nextInt();
            return new AbstractRefCounted() {

                @Override
                protected void closeInternal() {
                    // Do nothing
                    logger.info("Prevents this returning a non-collectible constant {}", number);
                }
            };
        }
    }

    private static class ReleasableTrackedObjectTestCase implements TrackedObjectTestCase {

        private Releasable releasable;

        @Override
        public void makeAssumptions() {
            assumeTrue("Releasable wrapper is a no-op when assertions are disabled", Assertions.ENABLED);
        }

        @Override
        public void createAndTrack(ReachabilityChecker reachabilityChecker) {
            Releasable releasable = reachabilityChecker.register(createReleasable());
            this.releasable = LeakTracker.wrap(releasable);
        }

        @Override
        public void nullifyReference() {
            releasable = null;
        }

        @Override
        public void closeLeak() {
            releasable.close();
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
