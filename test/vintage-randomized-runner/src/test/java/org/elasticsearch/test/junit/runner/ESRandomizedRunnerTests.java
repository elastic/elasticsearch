/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.junit.runner;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTCLASS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Tests how {@link ESRandomizedRunner} applies filters, by looking at the descriptions that remain after filtering a runner for a
 * fixture class: {@link RandomizedRunner#filter} prunes them, so nothing has to be run.
 */
@TestMethodProviders({ JUnit3MethodProvider.class })
public class ESRandomizedRunnerTests extends RandomizedTest {

    private static final String VINTAGE_LIKE_FILTER_PREFIX = ESRandomizedRunnerTests.class.getName() + "$VintageLike";

    /** The seed {@code tests.iters} adds to names: {@code testOne {p=a seed=[X:Y]}}, or {@code testOne {seed=[X:Y]}} without variants. */
    private static final Pattern SEED_IN_NAME = Pattern.compile(" ?seed=\\[[^\\]]*\\]");

    /**
     * Two methods with two variants each: {@link RandomizedRunner} creates a description per variant. The methods are found by their
     * name, as they are in {@code ESTestCase}.
     */
    @TestMethodProviders({ JUnit3MethodProvider.class })
    public static class ParameterizedFixture {

        public ParameterizedFixture(@Name("p") String p) {}

        @ParametersFactory
        public static Iterable<Object[]> parameters() {
            return List.of(new Object[] { "a" }, new Object[] { "b" });
        }

        public void testOne() {}

        public void testTwo() {}
    }

    @TestMethodProviders({ JUnit3MethodProvider.class })
    public static class PlainFixture {

        public void testOne() {}

        public void testTwo() {}

        public void testThree() {}
    }

    /** Says "run" for every description whose method name it was not told to reject, which includes the ones without parameters. */
    private abstract static class RejectingFilter extends Filter {
        private final Set<String> rejectedMethodNames;

        RejectingFilter(String... rejectedMethodNames) {
            this.rejectedMethodNames = Set.of(rejectedMethodNames);
        }

        @Override
        public boolean shouldRun(Description description) {
            return description.getMethodName() == null || rejectedMethodNames.contains(withoutSeed(description.getMethodName())) == false;
        }

        @Override
        public String describe() {
            return "rejects " + rejectedMethodNames;
        }
    }

    /** Stands in for the filters of the Vintage engine. */
    public static final class VintageLikeFilter extends RejectingFilter {
        VintageLikeFilter(String... rejectedMethodNames) {
            super(rejectedMethodNames);
        }
    }

    /** A filter that is not recognized as one built by the Vintage engine. */
    public static final class OtherFilter extends RejectingFilter {
        OtherFilter(String... rejectedMethodNames) {
            super(rejectedMethodNames);
        }
    }

    @Before
    public void assumeFixtureRunnersAreNotFiltered() {
        Assume.assumeTrue(
            "tests.class and tests.method would also filter the runners of the fixtures",
            System.getProperty(SYSPROP_TESTCLASS()) == null && System.getProperty(SYSPROP_TESTMETHOD()) == null
        );
    }

    /**
     * Documents what {@link ESRandomizedRunner} works around: {@link RandomizedRunner} also asks the filter about a description
     * with just the method name, which a filter that only rejects one variant accepts, and keeps the variant if either is accepted.
     */
    public void testRandomizedRunnerKeepsRejectedVariantWhileASiblingIsAccepted() throws Exception {
        RandomizedRunner runner = new RandomizedRunner(ParameterizedFixture.class);

        runner.filter(new OtherFilter("testOne {p=b}"));

        assertThat(
            leafNames(runner.getDescription()),
            containsInAnyOrder("testOne {p=a}", "testOne {p=b}", "testTwo {p=a}", "testTwo {p=b}")
        );
    }

    public void testRejectedVariantIsRemoved() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        runner.filter(new VintageLikeFilter("testOne {p=b}"));

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder("testOne {p=a}", "testTwo {p=a}", "testTwo {p=b}"));
    }

    public void testRejectedVariantsOfDifferentMethodsAreRemoved() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        runner.filter(new VintageLikeFilter("testOne {p=b}", "testTwo {p=a}"));

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder("testOne {p=a}", "testTwo {p=b}"));
    }

    public void testAllVariantsOfAMethodCanBeRejected() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        runner.filter(new VintageLikeFilter("testOne {p=a}", "testOne {p=b}"));

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder("testTwo {p=a}", "testTwo {p=b}"));
    }

    public void testAcceptingEverythingKeepsEveryVariant() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        runner.filter(new VintageLikeFilter());

        assertThat(
            leafNames(runner.getDescription()),
            containsInAnyOrder("testOne {p=a}", "testOne {p=b}", "testTwo {p=a}", "testTwo {p=b}")
        );
    }

    public void testRejectingEverythingLeavesNoTests() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        try {
            runner.filter(new VintageLikeFilter("testOne {p=a}", "testOne {p=b}", "testTwo {p=a}", "testTwo {p=b}"));
            Assert.fail("expected " + NoTestsRemainException.class + " to be thrown");
        } catch (NoTestsRemainException ignored) {}
    }

    public void testRejectedTestsWithoutParametersAreRemoved() throws Exception {
        ESRandomizedRunner runner = new ESRandomizedRunner(PlainFixture.class, VINTAGE_LIKE_FILTER_PREFIX);

        runner.filter(new VintageLikeFilter("testTwo"));

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder("testOne", "testThree"));
    }

    /** Only the filters of the Vintage engine are treated differently: all others behave as they do with a plain runner. */
    public void testFiltersNotBuiltByVintageAreAppliedAsRandomizedRunnerDoes() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        runner.filter(new OtherFilter("testOne {p=b}"));

        assertThat(
            leafNames(runner.getDescription()),
            containsInAnyOrder("testOne {p=a}", "testOne {p=b}", "testTwo {p=a}", "testTwo {p=b}")
        );
    }

    public void testFiltersNotBuiltByVintageStillRemoveTestsWithoutParameters() throws Exception {
        ESRandomizedRunner runner = new ESRandomizedRunner(PlainFixture.class, VINTAGE_LIKE_FILTER_PREFIX);

        runner.filter(new OtherFilter("testTwo"));

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder("testOne", "testThree"));
    }

    private static ESRandomizedRunner parameterizedFixtureRunner() throws Exception {
        return new ESRandomizedRunner(ParameterizedFixture.class, VINTAGE_LIKE_FILTER_PREFIX);
    }

    /** The method names of the tests of a description, without the seed {@code tests.iters} adds, so they can be compared. */
    private static Set<String> leafNames(Description description) {
        Set<String> names = new TreeSet<>();
        collectLeafNames(description, names);
        return names;
    }

    private static void collectLeafNames(Description description, Set<String> names) {
        if (description.isTest()) {
            // A method whose variants were all removed stays as a suite description without children, which is not a test
            if (description.getMethodName() != null) {
                names.add(withoutSeed(description.getMethodName()));
            }
        } else {
            for (Description child : description.getChildren()) {
                collectLeafNames(child, names);
            }
        }
    }

    private static String withoutSeed(String methodName) {
        return SEED_IN_NAME.matcher(methodName).replaceAll("").replace(" {}", "");
    }
}
