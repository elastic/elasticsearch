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

import org.elasticsearch.core.SuppressForbidden;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTCLASS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;

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
    public static class FailingFixture {

        public FailingFixture(@Name("p") String p) {}

        @ParametersFactory
        public static Iterable<Object[]> parameters() {
            return List.of(new Object[] { "a" }, new Object[] { "b" });
        }

        public void testPasses() {}

        public void testFails() {
            Assert.fail("expected");
        }

        public void testAssumptionFails() {
            Assume.assumeTrue(false);
        }
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

    /**
     * The iterations of a test are distinct descriptions, but their names do not carry the seed, so that name based filters, such as
     * Gradle's {@code --tests Class.method}, match them.
     */
    public void testIterationsAreDistinctDescriptionsWithTheSameName() throws Exception {
        Description description = withIterations(3, () -> new ESRandomizedRunner(PlainFixture.class, VINTAGE_SELECTING_FILTER_PREFIX))
            .getDescription();

        assertThat(description.getChildren().size(), equalTo(9));
        assertThat(new HashSet<>(description.getChildren()).size(), equalTo(9));
        for (Description child : description.getChildren()) {
            assertFalse(child.getMethodName(), SEED_IN_NAME.matcher(child.getMethodName()).find());
        }
    }

    /** What {@code --tests Class.method -Dtests.iters=N} does for a test without parameters. */
    public void testSelectingAMethodWorksWithIterations() throws Exception {
        ESRandomizedRunner runner = withIterations(3, () -> new ESRandomizedRunner(PlainFixture.class, VINTAGE_SELECTING_FILTER_PREFIX));

        runner.filter(new VintageLikeSelectingFilter("testOne"));

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder("testOne"));
        assertThat(runner.getDescription().getChildren().size(), equalTo(3));
    }

    /** The same for one variant of a parameterized test: the other variants of the method must not run. */
    public void testSelectingAMethodOfAParameterizedClassWorksWithIterations() throws Exception {
        ESRandomizedRunner runner = withIterations(
            3,
            () -> new ESRandomizedRunner(ParameterizedFixture.class, VINTAGE_SELECTING_FILTER_PREFIX)
        );

        runner.filter(new VintageLikeSelectingFilter("testOne {p=a}"));

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder("testOne {p=a}"));
        assertThat(runner.getDescription().getChildren().size(), equalTo(3));
    }

    /**
     * The Vintage engine is quadratic in the number of tests under suites that are not the root, so the tests must be its direct
     * children.
     */
    public void testDescriptionIsFlat() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        Description description = runner.getDescription();

        assertFlatChildren(description, "testOne {p=a}", "testOne {p=b}", "testTwo {p=a}", "testTwo {p=b}");
    }

    public void testDescriptionIsFlatAfterFiltering() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();
        runner.getDescription();

        runner.filter(new VintageLikeFilter("testOne {p=b}"));

        assertFlatChildren(runner.getDescription(), "testOne {p=a}", "testTwo {p=a}", "testTwo {p=b}");
    }

    public void testFlatDescriptionKeepsTheRootOfTheOriginalOne() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();

        Description original = new RandomizedRunner(ParameterizedFixture.class).getDescription();

        assertThat(runner.getDescription(), equalTo(original));
    }

    public void testDescriptionIsRebuiltWhenNoTestsRemain() throws Exception {
        ESRandomizedRunner runner = parameterizedFixtureRunner();
        runner.getDescription();

        try {
            runner.filter(new VintageLikeFilter("testOne {p=a}", "testOne {p=b}", "testTwo {p=a}", "testTwo {p=b}"));
            Assert.fail("expected " + NoTestsRemainException.class + " to be thrown");
        } catch (NoTestsRemainException ignored) {}

        assertThat(leafNames(runner.getDescription()), containsInAnyOrder());
    }

    public void testDescriptionOfTestsWithoutParametersIsFlat() throws Exception {
        ESRandomizedRunner runner = new ESRandomizedRunner(PlainFixture.class, VINTAGE_LIKE_FILTER_PREFIX);

        assertFlatChildren(runner.getDescription(), "testOne", "testTwo", "testThree");
    }

    /** Every event {@link RandomizedRunner} fires must be about the suite or a test of the flat description the engine discovered. */
    public void testRunEventsOnlyUseDescriptionsOfTheFlatTree() throws Exception {
        ESRandomizedRunner runner = new ESRandomizedRunner(FailingFixture.class, VINTAGE_LIKE_FILTER_PREFIX);
        Description root = runner.getDescription();
        Set<Description> known = new HashSet<>(root.getChildren());
        known.add(root);
        List<Description> fired = new ArrayList<>();
        List<Failure> failures = new ArrayList<>();
        RunNotifier notifier = new RunNotifier();
        notifier.addListener(new RunListener() {
            @Override
            public void testSuiteStarted(Description description) {
                fired.add(description);
            }

            @Override
            public void testSuiteFinished(Description description) {
                fired.add(description);
            }

            @Override
            public void testStarted(Description description) {
                fired.add(description);
            }

            @Override
            public void testFinished(Description description) {
                fired.add(description);
            }

            @Override
            public void testFailure(Failure failure) {
                fired.add(failure.getDescription());
                failures.add(failure);
            }

            @Override
            public void testAssumptionFailure(Failure failure) {
                fired.add(failure.getDescription());
            }
        });

        runner.run(notifier);

        Map<String, Long> discovered = countByName(root.getChildren());
        List<Description> failed = failures.stream().map(Failure::getDescription).toList();
        assertThat(
            countByName(failed),
            equalTo(Map.of("testFails {p=a}", discovered.get("testFails {p=a}"), "testFails {p=b}", discovered.get("testFails {p=b}")))
        );
        for (Description description : fired) {
            Assert.assertTrue("unknown description " + description, known.contains(description));
        }
    }

    /**
     * The direct children are exactly the given tests: no suites, no unnamed descriptions, and every test appears the same number of
     * times, which is more than once only when {@code tests.iters} repeats them.
     */
    private static void assertFlatChildren(Description description, String... expectedNames) {
        for (Description child : description.getChildren()) {
            Assert.assertTrue("not a test: " + child, child.isTest() && child.getMethodName() != null);
        }
        Map<String, Long> counts = countByName(description.getChildren());
        assertThat(counts.keySet(), containsInAnyOrder(expectedNames));
        assertThat(new HashSet<>(counts.values()).size(), equalTo(1));
    }

    private static Map<String, Long> countByName(Collection<Description> descriptions) {
        return descriptions.stream()
            .collect(Collectors.groupingBy(d -> withoutSeed(d.getMethodName()), TreeMap::new, Collectors.counting()));
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

    /**
     * Accepts only descriptions whose method name is exactly one of the given ones, as the filter Gradle builds for
     * {@code --tests Class.method} does. Unlike {@link RejectingFilter} it says "no" to anything else, so it also rejects descriptions
     * that carry the {@code seed=[...]} suffix added by {@code tests.iters}.
     */
    public static final class VintageLikeSelectingFilter extends Filter {
        private final Set<String> selected;

        VintageLikeSelectingFilter(String... selected) {
            this.selected = Set.of(selected);
        }

        @Override
        public boolean shouldRun(Description description) {
            // like Gradle's matcher, the suite is accepted when any of its children is
            if (description.getMethodName() == null) {
                return description.getChildren().stream().anyMatch(this::shouldRun);
            }
            return selected.contains(description.getMethodName());
        }

        @Override
        public String describe() {
            return "selects " + selected;
        }
    }

    private static final String VINTAGE_SELECTING_FILTER_PREFIX = ESRandomizedRunnerTests.class.getName() + "$VintageLikeSelecting";

    @SuppressForbidden(reason = "tests.iters is read from the system properties when a runner is created")
    private static <T> T withIterations(int iters, ThrowingSupplier<T> body) throws Exception {
        String key = "tests.iters";
        String previous = System.getProperty(key);
        System.setProperty(key, Integer.toString(iters));
        try {
            return body.get();
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
