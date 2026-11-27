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
import java.util.Set;

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

        private final RunNotifier actual;

        ExposingNotifier(RunNotifier actual) {
            this.actual = actual;
            // First, as the listeners of the actual notifier are told before the ones RandomizedRunner adds to its own
            super.addFirstListener(new TestMechanismFailureForwarder());
        }

        /**
         * A {@link RunNotifier} turns an exception thrown by a listener, such as one of the test class, into a failure of
         * {@link Description#TEST_MECHANISM}, and tells the listeners that did not throw about it once all of them have been told about
         * the event that caused it. It is told to the listeners of this notifier only, though, and the ones that report it to the
         * Vintage engine, which turns it into a failing test, are the listeners of the actual notifier. This listener, which does not
         * throw, hears of it with the others and passes it on.
         * <p>
         * It is the only way these failures get to the actual notifier, including when {@link RandomizedRunner} fires one itself.
         */
        private final class TestMechanismFailureForwarder extends RunListener {
            @Override
            public void testFailure(Failure failure) {
                if (failure.getDescription() == Description.TEST_MECHANISM) {
                    actual.fireTestFailure(failure);
                }
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
            // The failures of the test mechanism are passed on by the TestMechanismFailureForwarder
            if (failure.getDescription() != Description.TEST_MECHANISM) {
                actual.fireTestFailure(new Failure(exposeIfTest(failure.getDescription()), failure.getException()));
            }
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
