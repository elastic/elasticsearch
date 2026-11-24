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
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.Filterable;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunNotifier;
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
 */
public final class ESRandomizedRunner extends Runner implements Filterable {

    private static final String VINTAGE_ENGINE_PACKAGE = "org.junit.vintage.engine.";

    private final RandomizedRunner delegate;
    private final String vintageFilterClassNamePrefix;

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
        return delegate.getDescription();
    }

    @Override
    public void run(RunNotifier notifier) {
        delegate.run(notifier);
    }

    @Override
    public void filter(Filter filter) throws NoTestsRemainException {
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
    }

    private static void collectAccepted(Description description, Filter filter, Set<Description> accepted) {
        if (description.isTest()) {
            if (filter.shouldRun(description)) {
                accepted.add(description);
            }
        } else {
            for (Description child : description.getChildren()) {
                collectAccepted(child, filter, accepted);
            }
        }
    }
}
