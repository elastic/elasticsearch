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
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.elasticsearch.core.SuppressForbidden;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runner.notification.StoppedByUserException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTCLASS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        RandomizedRunner runner = plainRunnerWithoutIterations(ParameterizedFixture.class);

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
        ESRandomizedRunner runner = runnerWithoutIterations(PlainFixture.class, VINTAGE_LIKE_FILTER_PREFIX);

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
        ESRandomizedRunner runner = runnerWithoutIterations(PlainFixture.class, VINTAGE_LIKE_FILTER_PREFIX);

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

        Description original = plainRunnerWithoutIterations(ParameterizedFixture.class).getDescription();

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
        ESRandomizedRunner runner = runnerWithoutIterations(PlainFixture.class, VINTAGE_LIKE_FILTER_PREFIX);

        assertFlatChildren(runner.getDescription(), "testOne", "testTwo", "testThree");
    }

    /** Every event {@link RandomizedRunner} fires must be about the suite or a test of the flat description the engine discovered. */
    public void testRunEventsOnlyUseDescriptionsOfTheFlatTree() throws Exception {
        ESRandomizedRunner runner = runnerWithoutIterations(FailingFixture.class, VINTAGE_LIKE_FILTER_PREFIX);
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

    /** Records the descriptions of the events a runner fires, which of them are failures, and which tests were started. */
    private static class RecordedEvents extends RunListener {
        final List<Description> fired = new ArrayList<>();
        final List<Description> started = new ArrayList<>();
        final List<Description> finished = new ArrayList<>();
        final List<Description> failed = new ArrayList<>();

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
            started.add(description);
        }

        @Override
        public void testFinished(Description description) {
            fired.add(description);
            finished.add(description);
        }

        @Override
        public void testFailure(Failure failure) {
            fired.add(failure.getDescription());
            failed.add(failure.getDescription());
        }

        @Override
        public void testAssumptionFailure(Failure failure) {
            fired.add(failure.getDescription());
        }
    }

    /**
     * With {@code tests.iters} the events the runner passes on are about the tests as the platform discovered them, without the seed in
     * their names. The unique id of a description, which {@code equals} compares, keeps the seed, so what the test checks is the
     * method names, to be sure the events were translated: they are what Gradle reports.
     */
    public void testRunEventsDoNotHaveSeedsInTheirNamesWithIterations() throws Exception {
        ESRandomizedRunner runner = withIterations(3, () -> new ESRandomizedRunner(FailingFixture.class, VINTAGE_LIKE_FILTER_PREFIX));
        Description root = runner.getDescription();
        Set<Description> known = new HashSet<>(root.getChildren());
        known.add(root);
        RecordedEvents events = new RecordedEvents();
        RunNotifier notifier = new RunNotifier();
        notifier.addListener(events);

        runner.run(notifier);

        // 3 methods, 2 variants each, 3 iterations
        assertThat(root.getChildren().size(), equalTo(18));
        // Vintage pairs the events with the tests it discovered by the unique id of their descriptions
        assertThat(new HashSet<>(events.started), equalTo(new HashSet<>(root.getChildren())));
        assertThat(new HashSet<>(events.finished), equalTo(new HashSet<>(root.getChildren())));
        assertThat(events.started.size(), equalTo(18));
        assertThat(events.finished.size(), equalTo(18));
        assertThat(events.failed.size(), equalTo(6));
        assertTrue(known.containsAll(events.failed));
        for (Description description : events.fired) {
            Assert.assertTrue("unknown description " + description, known.contains(description));
            if (description.getMethodName() != null) {
                assertFalse(description.getMethodName(), SEED_IN_NAME.matcher(description.getMethodName()).find());
            }
        }
        assertThat(countByName(events.failed), equalTo(Map.of("testFails {p=a}", 3L, "testFails {p=b}", 3L)));
    }

    /** Without {@code tests.iters} the descriptions are the ones of {@link RandomizedRunner}, so are the events. */
    public void testRunEventsAreNotChangedWithoutIterations() throws Exception {
        ESRandomizedRunner runner = runnerWithoutIterations(FailingFixture.class, VINTAGE_LIKE_FILTER_PREFIX);
        RecordedEvents events = new RecordedEvents();
        RunNotifier notifier = new RunNotifier();
        notifier.addListener(events);

        runner.run(notifier);

        assertThat(events.started.size(), equalTo(6));
        assertThat(new HashSet<>(events.started), equalTo(new HashSet<>(runner.getDescription().getChildren())));
    }

    private static final AtomicInteger EXECUTED_TESTS = new AtomicInteger();

    @TestMethodProviders({ JUnit3MethodProvider.class })
    public static class CountingFixture {
        public void testOne() {
            EXECUTED_TESTS.incrementAndGet();
        }

        public void testTwo() {
            EXECUTED_TESTS.incrementAndGet();
        }

        public void testThree() {
            EXECUTED_TESTS.incrementAndGet();
        }
    }

    /** A notifier that, like the one of the Vintage engine, cancels the run by throwing from {@code fireTestStarted}. */
    private static final class CancellingNotifier extends RunNotifier {
        private final int allowedStarts;
        final List<Description> started = new ArrayList<>();

        CancellingNotifier(int allowedStarts) {
            this.allowedStarts = allowedStarts;
        }

        @Override
        public void fireTestStarted(Description description) throws StoppedByUserException {
            if (started.size() == allowedStarts) {
                throw new StoppedByUserException();
            }
            started.add(description);
            super.fireTestStarted(description);
        }
    }

    /**
     * What the actual notifier throws must reach {@link RandomizedRunner}, which stops the run because of it. Were it passed to a
     * listener, it would be turned into a failure and the tests would carry on.
     */
    public void testExceptionOfTheActualNotifierStopsTheRun() throws Exception {
        EXECUTED_TESTS.set(0);
        ESRandomizedRunner runner = withIterations(3, () -> new ESRandomizedRunner(CountingFixture.class, VINTAGE_LIKE_FILTER_PREFIX));
        CancellingNotifier notifier = new CancellingNotifier(2);

        try {
            runner.run(notifier);
        } catch (StoppedByUserException ignored) {
            // may propagate out of the run, as it does out of a plain RandomizedRunner
        }

        // 3 methods and 3 iterations make 9 tests
        assertThat(notifier.started.size(), equalTo(2));
        assertThat(EXECUTED_TESTS.get(), equalTo(2));
    }

    /** Registered with {@code @Listeners}, and rejects every test it is told about. */
    public static final class RejectingListener extends RunListener {
        @Override
        public void testStarted(Description description) {
            throw new IllegalStateException("rejects " + description.getMethodName());
        }
    }

    /** The failures that the peer of {@link RejectingListener} was told about. */
    private static final List<String> PEER_FAILURES = new CopyOnWriteArrayList<>();

    /** Registered with {@code @Listeners} after {@link RejectingListener}, which does not throw, and records the failures it hears of. */
    public static final class PeerListener extends RunListener {
        @Override
        public void testFailure(Failure failure) {
            PEER_FAILURES.add(failure.getDescription() + " : " + failure.getException());
        }
    }

    @Listeners({ RejectingListener.class, PeerListener.class })
    @TestMethodProviders({ JUnit3MethodProvider.class })
    public static class RejectedFixture {
        public void testOne() {}
    }

    /**
     * {@link RandomizedRunner} gives an exception thrown by one of the listeners to the wrapped notifier.
     * It has to reach the actual notifier from the wrapper, too, so that Vintage reports it as a test mechanism failure, and the other
     * listeners of the test class, which handled the event without a problem, have to hear of it, as they do from a plain
     * {@link RandomizedRunner}.
     */
    public void testExceptionOfAListenerOfTheTestClassIsReportedToTheActualNotifier() throws Exception {
        PEER_FAILURES.clear();
        List<Failure> plainFailures = runAndCollectFailures(plainRunnerWithoutIterations(RejectedFixture.class));
        List<String> plainPeerFailures = List.copyOf(PEER_FAILURES);

        PEER_FAILURES.clear();
        List<Failure> failures = runAndCollectFailures(runnerWithoutIterations(RejectedFixture.class, VINTAGE_LIKE_FILTER_PREFIX));
        List<String> peerFailures = List.copyOf(PEER_FAILURES);

        assertThat(plainFailures.size(), equalTo(1));
        assertThat(failures.size(), equalTo(1));
        assertThat(failures.get(0).getDescription(), equalTo(Description.TEST_MECHANISM));
        assertThat(failures.get(0).getException().getMessage(), equalTo(plainFailures.get(0).getException().getMessage()));
        assertThat(plainPeerFailures.size(), equalTo(1));
        assertThat(peerFailures, equalTo(plainPeerFailures));
    }

    private static List<Failure> runAndCollectFailures(Runner runner) {
        RecordedEvents events = new RecordedEvents();
        List<Failure> failures = new ArrayList<>();
        RunNotifier notifier = new RunNotifier();
        notifier.addListener(events);
        notifier.addListener(new RunListener() {
            @Override
            public void testFailure(Failure failure) {
                failures.add(failure);
            }
        });
        runner.run(notifier);
        return failures;
    }

    /** Method names the listeners saw while a {@link ListenedFixture} ran. */
    private static final List<String> LISTENED_NAMES = new CopyOnWriteArrayList<>();

    public static final class NameRecordingListener extends RunListener {
        @Override
        public void testStarted(Description description) {
            LISTENED_NAMES.add(description.getMethodName());
        }
    }

    @Listeners({ NameRecordingListener.class })
    @TestMethodProviders({ JUnit3MethodProvider.class })
    public static class ListenedFixture {
        public void testOne() {}
    }

    /**
     * The {@code @Listeners} of the test class, such as the one that prints how to reproduce a failure, need the seed of the iteration,
     * which is what makes it different from the others. They are called by {@link RandomizedRunner}, before the events are translated.
     */
    public void testListenersOfTheTestClassSeeTheSeedsOfIterations() throws Exception {
        LISTENED_NAMES.clear();
        ESRandomizedRunner runner = withIterations(3, () -> new ESRandomizedRunner(ListenedFixture.class, VINTAGE_LIKE_FILTER_PREFIX));
        RecordedEvents events = new RecordedEvents();
        RunNotifier notifier = new RunNotifier();
        notifier.addListener(events);

        runner.run(notifier);

        assertThat(LISTENED_NAMES.size(), equalTo(3));
        for (String name : LISTENED_NAMES) {
            Assert.assertNotNull(name, IterationSeeds.seedOf(name));
        }
        assertThat(events.started.size(), equalTo(3));
        for (Description description : events.started) {
            assertFalse(description.getMethodName(), SEED_IN_NAME.matcher(description.getMethodName()).find());
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
        return runnerWithoutIterations(ParameterizedFixture.class, VINTAGE_LIKE_FILTER_PREFIX);
    }

    /**
     * The build forwards {@code -Dtests.iters} to the JVM of these tests, and the runners read it when they are created, so a test that
     * relies on every test running once has to say so.
     */
    private static ESRandomizedRunner runnerWithoutIterations(Class<?> fixture, String vintageFilterClassNamePrefix) throws Exception {
        return withIterations(1, () -> new ESRandomizedRunner(fixture, vintageFilterClassNamePrefix));
    }

    /** The same as {@link #runnerWithoutIterations} for a runner that is not wrapped. */
    private static RandomizedRunner plainRunnerWithoutIterations(Class<?> fixture) throws Exception {
        return withIterations(1, () -> new RandomizedRunner(fixture));
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
