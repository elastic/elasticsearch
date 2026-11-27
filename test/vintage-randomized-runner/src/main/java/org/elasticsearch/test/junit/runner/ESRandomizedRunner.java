/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.junit.runner;

import com.carrotsearch.randomizedtesting.RandomizedRunner;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.Filterable;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runner.notification.StoppedByUserException;
import org.junit.runners.model.InitializationError;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link RandomizedRunner} wrapper that makes filters applied by the JUnit Vintage engine exact for parameterized tests.
 * <p>
 * {@link RandomizedRunner#filter} keeps a test as soon as the filter accepts <em>either</em> its full description
 * (e.g. {@code testFoo {p=b}}) <em>or</em> a simplified description that only carries the method name ({@code testFoo}).
 * The Gradle JUnit 4 integration compensates for that: a mute or retry exclusion of one parameterized test is expressed as two
 * patterns, one per description. The Vintage engine does not: when tests are removed from its descriptor tree (which is how
 * Gradle's {@code --tests} and {@code excludeTestsMatching} take effect on the JUnit Platform) it answers "run" for the
 * simplified description for as long as any sibling variant is still in the tree, so a removed variant is run anyway.
 * <p>
 * For filters built by Vintage this runner therefore evaluates the filter once per real test description and only lets the
 * accepted ones through.
 * <p>
 * The description is also flattened to the suite and its tests, dropping the per-method suites {@link RandomizedRunner} adds. It runs
 * tests in random order, so with those suites the Vintage engine keeps one per method in progress at once, and on every test start it
 * copies and scans all their children: quadratic in the number of tests, which made suites with tens of thousands of tests time out.
 */
public final class ESRandomizedRunner extends Runner implements Filterable {

    private static final String VINTAGE_ENGINE_PACKAGE = "org.junit.vintage.engine.";

    private final RandomizedRunner delegate;
    private final String vintageFilterClassNamePrefix;
    private volatile Description flatDescription;

    @SuppressWarnings("unused") // main Runner constructor
    public ESRandomizedRunner(Class<?> suiteClass) throws InitializationError {
        this(suiteClass, VINTAGE_ENGINE_PACKAGE);
    }

    // visible for testing
    ESRandomizedRunner(Class<?> suiteClass, String vintageFilterClassNamePrefix) throws InitializationError {
        this.delegate = new RandomizedRunner(suiteClass);
        this.vintageFilterClassNamePrefix = vintageFilterClassNamePrefix;
    }

    @Override
    public Description getDescription() {
        Description flat = flatDescription;
        if (flat == null) {
            flat = flatten(delegate.getDescription());
            flatDescription = flat;
        }
        return flat;
    }

    private static Description flatten(Description suite) {
        Description flat = suite.childlessCopy();
        addLeaves(suite, flat);
        return flat;
    }

    private static void addLeaves(Description description, Description flat) {
        for (Description child : description.getChildren()) {
            if (child.isTest()) {
                if (child.getMethodName() != null) {
                    flat.addChild(expose(child));
                }
            } else {
                addLeaves(child, flat);
            }
        }
    }

    @Override
    public void run(RunNotifier actualNotifier) {
        delegate.run(new ExposingNotifier(actualNotifier));
    }

    /**
     * The notifier {@link RandomizedRunner} fires its events on. The actual notifier is called directly, and first, instead of through
     * a listener, so that what it throws gets to {@link RandomizedRunner} as it would without this wrapper. A listener of
     * a {@link RunNotifier} has its exceptions turned into a failure, and the Vintage engine cancels a run by throwing
     * {@link StoppedByUserException} from {@code fireTestStarted}. Being first also keeps the listeners from hearing about a test that
     * was canceled before it started.
     * <p>
     * Every {@code fire*()} method of {@link RunNotifier} is overridden, even though {@link RandomizedRunner} only fires the
     * events of tests (started, finished, failed, assumption failed and ignored).
     */
    private static final class ExposingNotifier extends RunNotifier {

        @FunctionalInterface
        private interface ListenerCall {
            void run() throws Exception;
        }

        private final RunNotifier actual;

        /** Key: actual listener, value: wrapped listener */
        private final Map<RunListener, FailureReportingListener> wrappedListeners = new ConcurrentHashMap<>();

        ExposingNotifier(RunNotifier actual) {
            this.actual = actual;
        }

        /**
         * A {@link RunNotifier} turns an exception thrown by a listener into a failure of {@link Description#TEST_MECHANISM}, which it
         * tells the other listeners of the same notifier about. The listeners of the wrapper would never notify the listeners of the actual
         * (wrapped) notifier, whose listeners report the failure to the Vintage test engine; the {@link FailureReportingListener} forwards
         * the errors to the actual notifier (through the wrapper).
         */
        @Override
        public void addListener(RunListener listener) {
            super.addListener(wrappedListeners.computeIfAbsent(listener, FailureReportingListener::new));
        }

        @Override
        public void addFirstListener(RunListener listener) {
            super.addFirstListener(wrappedListeners.computeIfAbsent(listener, FailureReportingListener::new));
        }

        @Override
        public void removeListener(RunListener listener) {
            FailureReportingListener wrapped = wrappedListeners.remove(listener);
            super.removeListener(wrapped == null ? listener : wrapped);
        }

        /** Passes on the events to a listener, and gives what it throws to the notifier as a failure of the test mechanism. */
        private final class FailureReportingListener extends RunListener {
            private final RunListener listener;

            FailureReportingListener(RunListener listener) {
                this.listener = listener;
            }

            private void notifyListener(ListenerCall call) {
                try {
                    call.run();
                } catch (Exception e) {
                    ExposingNotifier.this.fireTestFailure(new Failure(Description.TEST_MECHANISM, e));
                }
            }

            @Override
            public void testRunStarted(Description description) {
                notifyListener(() -> listener.testRunStarted(description));
            }

            @Override
            public void testRunFinished(Result result) {
                notifyListener(() -> listener.testRunFinished(result));
            }

            @Override
            public void testSuiteStarted(Description description) {
                notifyListener(() -> listener.testSuiteStarted(description));
            }

            @Override
            public void testSuiteFinished(Description description) {
                notifyListener(() -> listener.testSuiteFinished(description));
            }

            @Override
            public void testStarted(Description description) {
                notifyListener(() -> listener.testStarted(description));
            }

            @Override
            public void testFinished(Description description) {
                notifyListener(() -> listener.testFinished(description));
            }

            @Override
            public void testFailure(Failure failure) {
                notifyListener(() -> listener.testFailure(failure));
            }

            @Override
            public void testAssumptionFailure(Failure failure) {
                notifyListener(() -> listener.testAssumptionFailure(failure));
            }

            @Override
            public void testIgnored(Description description) {
                notifyListener(() -> listener.testIgnored(description));
            }
        }

        @Override
        public void fireTestRunStarted(Description description) {
            actual.fireTestRunStarted(description);
            super.fireTestRunStarted(description);
        }

        @Override
        public void fireTestRunFinished(Result result) {
            actual.fireTestRunFinished(result);
            super.fireTestRunFinished(result);
        }

        @Override
        public void fireTestSuiteStarted(Description description) {
            actual.fireTestSuiteStarted(exposeIfTest(description));
            super.fireTestSuiteStarted(description);
        }

        @Override
        public void fireTestSuiteFinished(Description description) {
            actual.fireTestSuiteFinished(exposeIfTest(description));
            super.fireTestSuiteFinished(description);
        }

        @Override
        public void fireTestStarted(Description description) throws StoppedByUserException {
            actual.fireTestStarted(exposeIfTest(description));
            super.fireTestStarted(description);
        }

        @Override
        public void fireTestFinished(Description description) {
            actual.fireTestFinished(exposeIfTest(description));
            super.fireTestFinished(description);
        }

        @Override
        public void fireTestFailure(Failure failure) {
            actual.fireTestFailure(new Failure(exposeIfTest(failure.getDescription()), failure.getException()));
            super.fireTestFailure(failure);
        }

        @Override
        public void fireTestAssumptionFailed(Failure failure) {
            actual.fireTestAssumptionFailed(new Failure(exposeIfTest(failure.getDescription()), failure.getException()));
            super.fireTestAssumptionFailed(failure);
        }

        @Override
        public void fireTestIgnored(Description description) {
            actual.fireTestIgnored(exposeIfTest(description));
            super.fireTestIgnored(description);
        }

        @Override
        public void pleaseStop() {
            actual.pleaseStop();
            super.pleaseStop();
        }
    }

    @Override
    public void filter(Filter filter) throws NoTestsRemainException {
        try {
            if (filter.getClass().getName().startsWith(vintageFilterClassNamePrefix) == false) {
                delegate.filter(filter);
                return;
            }
            Set<Description> accepted = new HashSet<>();
            collectAccepted(delegate.getDescription(), filter, accepted);
            delegate.filter(new Filter() {
                @Override
                public boolean shouldRun(Description description) {
                    // The simplified (method name only) descriptions RandomizedRunner also asks about are not leaves of the
                    // description tree for parameterized tests, so they are never accepted here.
                    return accepted.contains(description);
                }

                @Override
                public String describe() {
                    return filter.describe();
                }
            });
        } finally {
            flatDescription = null;
        }
    }

    private static void collectAccepted(Description description, Filter filter, Set<Description> accepted) {
        if (description.isTest()) {
            if (description.getMethodName() != null && filter.shouldRun(expose(description))) {
                accepted.add(description);
            }
        } else {
            for (Description child : description.getChildren()) {
                collectAccepted(child, filter, accepted);
            }
        }
    }

    private static Description exposeIfTest(Description description) {
        return description.isTest() && description.getMethodName() != null ? expose(description) : description;
    }

    /**
     * The description of a test as seen by the JUnit Platform. With {@code tests.iters} {@link RandomizedRunner} puts the seed of each
     * iteration in the name of the test ({@code testFoo {p=b seed=[X:Y]}}), so Gradle's {@code --tests Class.method} would not match any
     * of them. The seed is moved to the description, which is a part of Vintage UniqueId -- it is what tells the iterations apart when they
     * are filtered.
     * <p>
     * The description is made from the class name, so it has none of the annotations of the test method that the one
     * {@link RandomizedRunner} creates from the class has. Nothing in the Vintage engine or Gradle reads them today.
     */
    private static Description expose(Description test) {
        String name = IterationSeeds.withoutSeed(test.getMethodName());
        if (name.equals(test.getMethodName())) {
            return test;
        }
        return Description.createTestDescription(test.getClassName(), name, test.getDisplayName());
    }
}
