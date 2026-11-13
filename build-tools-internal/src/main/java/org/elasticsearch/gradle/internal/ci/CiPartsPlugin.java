/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * Registers {@code checkPart1..N}, which split a project's {@code check} across CI jobs as listed in the
 * checked-in {@link CiPartAssignment}, and likewise {@code bcUpgradeTestPart1..N}, which split the
 * {@code bcUpgradeTest} tasks of all projects across the PR upgrade jobs using an assignment of their own.
 *
 * <p>{@code checkPartN} depends on the subset of {@code check}'s dependencies that belongs to part N: the test
 * tasks assigned to N, and, when N is the project's home part, everything else {@code check} runs (precommit
 * and other non-test verification), except work that depends on test tasks, which runs in the parts of those
 * tests. Lifecycle tasks are flattened so that test tasks behind them (for example
 * an aggregate of several test tasks) are still placed individually. Because the selection is derived from
 * {@code check} itself, running every part runs exactly what {@code check} runs.
 *
 * <p>A test task the assignment splits into shards is the same Gradle task in every part that runs one of its
 * shards; when the build is asked for {@code checkPartN} tasks, the plugin narrows each split task to the shards
 * of the requested parts, either by excluding the test classes of other shards or, for YAML suites, through the
 * {@code tests.rest.shard} system property read by {@code ESClientYamlSuiteTestCase}. Every other invocation
 * (including {@code check} and reproducing a failure) runs the whole task.
 *
 * <p>Gradle starts ready tasks in path order, so a long test task whose path sorts late can start long after
 * the part began and end it late. When part tasks are requested, the other tasks of a part should run after
 * the part's {@link CiPartAssignment#runFirst() run-first} test tasks, and those after the longer ones. That is a
 * soft ordering: it moves long tasks (and what they depend on) forward in the execution plan, but a worker that has
 * nothing else ready still takes a short task.
 *
 * <p>When part tasks are requested, test tasks that start clusters are also limited by {@link CiHeavyTestThrottle}.
 * Run-first tasks then take the throttle's slots first, longest first.
 *
 * <p>The {@code bcUpgradeTest} suite works the same way with {@code bcUpgradeTest} in place of {@code check}.
 * Splitting it by class is safe because each upgrade test class starts and upgrades its own cluster. So does the
 * ES|QL release test suite ({@code releaseTestCheckEsqlPartN}) with {@code esqlReleaseTestCheck}, which the build
 * registers on the projects whose release-build checks the ES|QL release test jobs run.
 *
 * <p>The BWC suite ({@code v<version>#bwcTestPartN}) splits {@code v<version>#bwcTest} for whichever BWC version is
 * requested. Its tasks are created by a task rule, since the versions differ between branches, and one assignment
 * serves every version: its keys name the version {@value #VERSION_PLACEHOLDER}, for example
 * {@code :qa:rolling-upgrade:v{V}#bwcTest}. Some BWC test tasks depend on other test tasks (rolling upgrade's
 * {@code bwcTest} on its {@code yamlBwcTest}); those are placed on their own and skipped in the requested parts
 * that don't hold them, or every part running a shard of the dependent task would run them again.
 */
public abstract class CiPartsPlugin implements Plugin<Project> {

    public static final String ASSIGNMENT_FILE = ".ci/test-parts/assignment.json";
    public static final String CHECK_PART_TASK_PREFIX = "checkPart";
    public static final String BC_UPGRADE_ASSIGNMENT_FILE = ".ci/test-parts/bc-upgrade/assignment.json";
    public static final String BC_UPGRADE_PART_TASK_PREFIX = "bcUpgradeTestPart";
    public static final String ESQL_RELEASE_ASSIGNMENT_FILE = ".ci/test-parts/esql-release/assignment.json";
    public static final String ESQL_RELEASE_PART_TASK_PREFIX = "releaseTestCheckEsqlPart";
    public static final String BWC_ASSIGNMENT_FILE = ".ci/test-parts/bwc/assignment.json";
    public static final String BWC_PART_TASK_PREFIX = "bwcTestPart";
    /** Stands for the BWC version in the BWC suite's assignment keys. */
    public static final String VERSION_PLACEHOLDER = "{V}";
    static final String REST_TESTS_SHARD_PROPERTY = "tests.rest.shard";
    private static final String VERSION_REGEX = "\\d+\\.\\d+\\.\\d+";
    /** The BWC version a task name starts with, as in {@code v9.1.0#bwcTest}. */
    private static final Pattern VERSIONED_TASK_NAME = Pattern.compile("^v(" + VERSION_REGEX + ")#");

    /**
     * A set of part tasks: {@code <taskPrefix>N} runs the share of {@code rootTask} that the assignment in
     * {@code assignmentFile} gives part N, and, if {@code assembles}, the home part also runs {@code assemble} when
     * {@link CiPartsExtension#getIncludeAssemble()} asks for it. A {@code versioned} suite has a set of part tasks per
     * BWC version: {@code v<version>#<taskPrefix>N} runs part N of {@code v<version>#<rootTask>}.
     */
    record Suite(String taskPrefix, String rootTask, String assignmentFile, boolean assembles, boolean versioned) {
        /** Matches a requested part task: group 1 is the version ("" for a suite without versions), group 2 the part. */
        Pattern partTaskPattern() {
            return Pattern.compile("(?:^|:)" + (versioned ? "v(" + VERSION_REGEX + ")#" : "()") + taskPrefix + "(\\d+)$");
        }

        String partTaskName(String version, int part) {
            return (versioned ? "v" + version + "#" : "") + taskPrefix + part;
        }

        String rootTaskName(String version) {
            return (versioned ? "v" + version + "#" : "") + rootTask;
        }
    }

    /**
     * Turns the task paths of one BWC version into assignment keys and back. For a suite without versions
     * ({@code version} empty) keys are the task paths.
     */
    record Keys(String version) {
        static final Keys NONE = new Keys("");

        String key(String taskPath) {
            return version.isEmpty() ? taskPath : taskPath.replace(":v" + version + "#", ":v" + VERSION_PLACEHOLDER + "#");
        }

        String path(String key) {
            return version.isEmpty() ? key : key.replace(":v" + VERSION_PLACEHOLDER + "#", ":v" + version + "#");
        }
    }

    static final Suite CHECK = new Suite(CHECK_PART_TASK_PREFIX, LifecycleBasePlugin.CHECK_TASK_NAME, ASSIGNMENT_FILE, true, false);
    static final Suite BC_UPGRADE = new Suite(BC_UPGRADE_PART_TASK_PREFIX, "bcUpgradeTest", BC_UPGRADE_ASSIGNMENT_FILE, true, false);
    // the ES|QL release test jobs are release builds that only test, so they never assemble
    static final Suite ESQL_RELEASE = new Suite(
        ESQL_RELEASE_PART_TASK_PREFIX,
        "esqlReleaseTestCheck",
        ESQL_RELEASE_ASSIGNMENT_FILE,
        false,
        false
    );
    static final Suite BWC = new Suite(BWC_PART_TASK_PREFIX, "bwcTest", BWC_ASSIGNMENT_FILE, false, true);
    static final List<Suite> SUITES = List.of(CHECK, BC_UPGRADE, ESQL_RELEASE, BWC);

    @Inject
    protected abstract ProviderFactory getProviders();

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);
        CiPartsExtension extension = project.getExtensions().create("ciParts", CiPartsExtension.class);
        extension.getIncludeAssemble().convention(false);
        List<String> taskNames = project.getGradle().getStartParameter().getTaskNames();
        for (Suite suite : SUITES) {
            apply(project, suite, assignmentService(project, suite).get(), extension, taskNames);
        }
        if (SUITES.stream().anyMatch(suite -> requestedParts(suite, taskNames).isEmpty() == false)) {
            throttleHeavyTests(project);
        }
    }

    /**
     * Part tasks are what CI runs, so only then are test tasks that start clusters limited to
     * {@link CiHeavyTestThrottle}'s share of the workers; other builds (a developer's, say) are not throttled.
     */
    private void throttleHeavyTests(Project project) {
        int maxWorkers = project.getGradle().getStartParameter().getMaxWorkerCount();
        Provider<String> override = getProviders().systemProperty(CiHeavyTestThrottle.MAX_PARALLEL_PROPERTY);
        int limit = override.isPresent() ? Integer.parseInt(override.get()) : CiHeavyTestThrottle.defaultLimit(maxWorkers);
        if (limit < 0) {
            throw new IllegalArgumentException(CiHeavyTestThrottle.MAX_PARALLEL_PROPERTY + " must not be negative but was " + limit);
        }
        if (limit == 0) {
            return;
        }
        Provider<CiHeavyTestThrottle> throttle = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("ciHeavyTestThrottle", CiHeavyTestThrottle.class, spec -> spec.getMaxParallelUsages().set(limit));
        project.getTasks().withType(Test.class).configureEach(test -> {
            if (CiHeavyTestThrottle.isHeavy(test.getName())) {
                test.usesService(throttle);
            }
        });
    }

    private Provider<CiPartsBuildService> assignmentService(Project project, Suite suite) {
        return project.getGradle()
            .getSharedServices()
            .registerIfAbsent("ciParts-" + suite.taskPrefix(), CiPartsBuildService.class, spec -> {
                spec.getParameters().getAssignmentFile().set(suite.assignmentFile());
                spec.getParameters()
                    .getAssignmentJson()
                    .set(getProviders().fileContents(project.getLayout().getSettingsDirectory().file(suite.assignmentFile())).getAsText());
            });
    }

    private static void apply(
        Project project,
        Suite suite,
        CiPartsBuildService service,
        CiPartsExtension extension,
        List<String> taskNames
    ) {
        CiPartAssignment assignment = service.assignment();
        Map<String, Set<Integer>> requested = requestedPartsByVersion(suite, taskNames);
        if (requested.isEmpty() == false && assignment.runFirst().isEmpty() == false) {
            service.registerTasks(project.getPath(), project.getTasks());
        }
        if (suite.versioned()) {
            Pattern partTask = Pattern.compile("^v(" + VERSION_REGEX + ")#" + suite.taskPrefix() + "(\\d+)$");
            project.getTasks().addRule("Pattern: v<BWC version>#" + suite.taskPrefix() + "<part>", name -> {
                Matcher matcher = partTask.matcher(name);
                if (matcher.matches()) {
                    String version = matcher.group(1);
                    int part = Integer.parseInt(matcher.group(2));
                    if (part >= 1 && part <= assignment.parts()) {
                        Set<Integer> requestedParts = requested.getOrDefault(version, Set.of());
                        // realized here: the lookup that fired the rule only finds tasks that exist, not registered ones
                        registerPartTask(project, suite, service, extension, new Keys(version), part, requestedParts).get();
                    }
                }
            });
        } else {
            for (int part = 1; part <= assignment.parts(); part++) {
                registerPartTask(project, suite, service, extension, Keys.NONE, part, requested.getOrDefault("", Set.of()));
            }
        }

        if (requested.isEmpty() == false) {
            project.getTasks().withType(Test.class).configureEach(test -> configureShards(test, suite, assignment, requested));
        }
    }

    private static TaskProvider<Task> registerPartTask(
        Project project,
        Suite suite,
        CiPartsBuildService service,
        CiPartsExtension extension,
        Keys keys,
        int part,
        Set<Integer> requestedParts
    ) {
        CiPartAssignment assignment = service.assignment();
        return project.getTasks().register(suite.partTaskName(keys.version(), part), t -> {
            t.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
            t.setDescription(
                "Runs the share of '"
                    + suite.rootTaskName(keys.version())
                    + "' assigned to CI part "
                    + part
                    + " of "
                    + assignment.parts()
                    + "."
            );
            t.dependsOn((Callable<List<Task>>) () -> {
                List<Test> chained = new ArrayList<>();
                List<Task> selected = tasksForPart(project, suite, assignment, extension, keys, part, chained);
                if (requestedParts.contains(part)) {
                    skipChainedTests(chained, assignment, keys, requestedParts);
                    if (assignment.runFirst().isEmpty() == false) {
                        orderLongestFirst(project, selected, runFirstInPart(assignment, keys, part), service);
                    }
                }
                return selected;
            });
        });
    }

    /** The run-first test tasks that run (some shards) in {@code part}, longest first, as task paths. */
    static List<String> runFirstInPart(CiPartAssignment assignment, Keys keys, int part) {
        return assignment.runFirst().stream().filter(key -> assignment.runsInPart(key, part)).map(keys::path).toList();
    }

    /**
     * Makes each of {@code selected} (one project's share of a part) run after the part's run-first tasks that are
     * longer than it: all of them for a task that is not run-first itself. Run-first tasks that do not exist (for
     * example ones removed since the assignment was generated) are skipped, and so are selected tasks of other
     * projects or builds (the root project's {@code check} reaches tasks of included builds, which have already run
     * by now and whose ordering can no longer change); their own project orders them.
     */
    static void orderLongestFirst(Project project, List<Task> selected, List<String> runFirst, CiPartsBuildService service) {
        List<TaskProvider<Task>> longTasks = new ArrayList<>();
        List<String> longPaths = new ArrayList<>();
        for (String taskPath : runFirst) {
            service.findTask(taskPath).ifPresent(provider -> {
                longTasks.add(provider);
                longPaths.add(taskPath);
            });
        }
        for (Task task : selected) {
            if (task.getProject() != project) {
                continue;
            }
            int index = longPaths.indexOf(task.getPath());
            List<TaskProvider<Task>> before = index < 0 ? longTasks : longTasks.subList(0, index);
            if (before.isEmpty() == false) {
                task.shouldRunAfter(List.copyOf(before));
            }
        }
    }

    /**
     * Skips the test tasks that another test task of the part depends on but that the requested parts don't hold:
     * Gradle runs a task's dependencies wherever the task runs, so without this they would run in every part that runs
     * the dependent task (or one of its shards), as well as in their own.
     */
    static void skipChainedTests(List<Test> chained, CiPartAssignment assignment, Keys keys, Set<Integer> requestedParts) {
        for (Test test : chained) {
            if (assignment.shardsInParts(keys.key(test.getPath()), requestedParts).isEmpty()) {
                test.onlyIf("it runs in another CI part", new RunsInAnotherPart());
            }
        }
    }

    /** A class rather than a lambda, so that the configuration cache can store it. */
    private static final class RunsInAnotherPart implements Spec<Task> {
        @Override
        public boolean isSatisfiedBy(Task task) {
            return false;
        }
    }

    /**
     * The parts named by part task requests of {@code suite}, e.g. {@code checkPart3} or {@code :server:checkPart3}, of
     * any BWC version for a versioned suite.
     */
    static Set<Integer> requestedParts(Suite suite, List<String> taskNames) {
        return requestedPartsByVersion(suite, taskNames).values()
            .stream()
            .flatMap(Set::stream)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * The parts named by part task requests of {@code suite} by BWC version, e.g. {@code v9.1.0#bwcTestPart3}; a suite
     * without versions has them under {@code ""}.
     */
    static Map<String, Set<Integer>> requestedPartsByVersion(Suite suite, List<String> taskNames) {
        Pattern pattern = suite.partTaskPattern();
        Map<String, Set<Integer>> parts = new TreeMap<>();
        for (String name : taskNames) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.find()) {
                parts.computeIfAbsent(matcher.group(1), v -> new TreeSet<>()).add(Integer.parseInt(matcher.group(2)));
            }
        }
        return parts;
    }

    static void configureShards(Test test, Suite suite, CiPartAssignment assignment, Map<String, Set<Integer>> requested) {
        Keys keys = Keys.NONE;
        if (suite.versioned()) {
            Matcher matcher = VERSIONED_TASK_NAME.matcher(test.getName());
            if (matcher.find() == false) {
                return;
            }
            keys = new Keys(matcher.group(1));
        }
        Set<Integer> requestedParts = requested.get(keys.version());
        if (requestedParts == null) {
            return;
        }
        String key = keys.key(test.getPath());
        CiPartAssignment.Split split = assignment.split(key);
        if (split == null) {
            return;
        }
        Set<Integer> shards = assignment.shardsInParts(key, requestedParts);
        if (shards.isEmpty() || shards.size() == split.count()) {
            // the task runs whole: either all of its shards are requested, or it is only in the graph for another reason
            return;
        }
        String shardSpec = split.count() + ":" + shards.stream().map(String::valueOf).collect(Collectors.joining(","));
        // through Task, since Test#getInputs() is declared with an internal Gradle return type
        Task task = test;
        task.getInputs().property("ciShards", split.by() + "=" + shardSpec);
        switch (split.by()) {
            case CLASS -> test.exclude(new ClassShardSpec(split.count(), Set.copyOf(shards)));
            case YAML_FILE -> test.systemProperty(REST_TESTS_SHARD_PROPERTY, shardSpec);
        }
    }

    static List<Task> tasksForPart(Project project, Suite suite, CiPartAssignment assignment, CiPartsExtension extension, int part) {
        return tasksForPart(project, suite, assignment, extension, Keys.NONE, part, new ArrayList<>());
    }

    /**
     * The tasks of {@code project} in {@code part}. Collects into {@code chained} the test tasks that are dependencies
     * of other test tasks (only followed in versioned suites, where BWC test tasks are chained like that; elsewhere
     * resolving every test task's dependencies would cost graph-building time for nothing).
     */
    static List<Task> tasksForPart(
        Project project,
        Suite suite,
        CiPartAssignment assignment,
        CiPartsExtension extension,
        Keys keys,
        int part,
        List<Test> chained
    ) {
        boolean home = assignment.homePart(project.getPath()) == part;
        List<Task> selected = new ArrayList<>();
        // most projects have no bcUpgradeTest (or bwcTest); their part tasks are empty (but for assemble below)
        Task root = project.getTasks().findByName(suite.rootTaskName(keys.version()));
        if (root != null) {
            Selection selection = new Selection(project, assignment, keys, part, home, suite.versioned(), selected, chained);
            selection.select(root);
        }
        if (home && suite.assembles() && extension.getIncludeAssemble().get()) {
            selected.add(project.getTasks().getByName(LifecycleBasePlugin.ASSEMBLE_TASK_NAME));
        }
        return selected;
    }

    /** One walk of a root task's dependencies, collecting the tasks of one part. */
    private record Selection(
        Project project,
        CiPartAssignment assignment,
        Keys keys,
        int part,
        boolean home,
        boolean followTestDependencies,
        List<Task> selected,
        List<Test> chained,
        Set<Task> visited
    ) {
        Selection(
            Project project,
            CiPartAssignment assignment,
            Keys keys,
            int part,
            boolean home,
            boolean followTestDependencies,
            List<Task> selected,
            List<Test> chained
        ) {
            this(project, assignment, keys, part, home, followTestDependencies, selected, chained, new HashSet<>());
        }

        void select(Task task) {
            if (visited.add(task) == false) {
                return;
            }
            if (task instanceof Test) {
                if (assignment.runsInPart(keys.key(task.getPath()), part)) {
                    selected.add(task);
                }
                if (followTestDependencies) {
                    for (Task dependency : task.getTaskDependencies().getDependencies(task)) {
                        if (dependency instanceof Test test && dependency.getProject() == project) {
                            chained.add(test);
                            select(test);
                        }
                    }
                }
            } else if (task.getActions().isEmpty()) {
                for (Task dependency : task.getTaskDependencies().getDependencies(task)) {
                    select(dependency);
                }
            } else {
                // work that needs test results (a coverage report, say) runs where those tests run; in the home part it
                // would pull the whole test task in again. Only this project's tasks are inspected: the root project's
                // check also reaches tasks of included builds, whose dependencies cannot be resolved from here.
                List<? extends Task> tests = task.getProject() != project
                    ? List.of()
                    : task.getTaskDependencies().getDependencies(task).stream().filter(dependency -> dependency instanceof Test).toList();
                if (tests.isEmpty() ? home : tests.stream().anyMatch(test -> assignment.runsInPart(keys.key(test.getPath()), part))) {
                    selected.add(task);
                }
            }
        }
    }
}
