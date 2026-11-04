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
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * <p>The {@code bcUpgradeTest} suite works the same way with {@code bcUpgradeTest} in place of {@code check}.
 * Splitting it by class is safe because each upgrade test class starts and upgrades its own cluster.
 */
public abstract class CiPartsPlugin implements Plugin<Project> {

    public static final String ASSIGNMENT_FILE = ".ci/test-parts/assignment.json";
    public static final String CHECK_PART_TASK_PREFIX = "checkPart";
    public static final String BC_UPGRADE_ASSIGNMENT_FILE = ".ci/test-parts/bc-upgrade/assignment.json";
    public static final String BC_UPGRADE_PART_TASK_PREFIX = "bcUpgradeTestPart";
    static final String REST_TESTS_SHARD_PROPERTY = "tests.rest.shard";

    /**
     * A set of part tasks: {@code <taskPrefix>N} runs the share of {@code rootTask} that the assignment in
     * {@code assignmentFile} gives part N.
     */
    record Suite(String taskPrefix, String rootTask, String assignmentFile) {
        Pattern partTaskPattern() {
            return Pattern.compile("(?:^|:)" + taskPrefix + "(\\d+)$");
        }
    }

    static final Suite CHECK = new Suite(CHECK_PART_TASK_PREFIX, LifecycleBasePlugin.CHECK_TASK_NAME, ASSIGNMENT_FILE);
    static final Suite BC_UPGRADE = new Suite(BC_UPGRADE_PART_TASK_PREFIX, "bcUpgradeTest", BC_UPGRADE_ASSIGNMENT_FILE);
    static final List<Suite> SUITES = List.of(CHECK, BC_UPGRADE);

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
        Set<Integer> requestedParts = requestedParts(suite, taskNames);
        boolean orderLongestFirst = requestedParts.isEmpty() == false && assignment.runFirst().isEmpty() == false;
        if (orderLongestFirst) {
            service.registerTasks(project.getPath(), project.getTasks());
        }
        for (int part = 1; part <= assignment.parts(); part++) {
            int thisPart = part;
            project.getTasks().register(suite.taskPrefix() + part, t -> {
                t.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
                t.setDescription(
                    "Runs the share of '" + suite.rootTask() + "' assigned to CI part " + thisPart + " of " + assignment.parts() + "."
                );
                t.dependsOn((Callable<List<Task>>) () -> {
                    List<Task> selected = tasksForPart(project, suite, assignment, extension, thisPart);
                    if (orderLongestFirst && requestedParts.contains(thisPart)) {
                        orderLongestFirst(project, selected, runFirstInPart(assignment, thisPart), service);
                    }
                    return selected;
                });
            });
        }

        if (requestedParts.isEmpty() == false) {
            project.getTasks().withType(Test.class).configureEach(test -> configureShards(test, assignment, requestedParts));
        }
    }

    /** The run-first test tasks that run (some shards) in {@code part}, longest first. */
    static List<String> runFirstInPart(CiPartAssignment assignment, int part) {
        return assignment.runFirst().stream().filter(taskPath -> assignment.runsInPart(taskPath, part)).toList();
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

    /** The parts named by part task requests of {@code suite}, e.g. {@code checkPart3} or {@code :server:checkPart3}. */
    static Set<Integer> requestedParts(Suite suite, List<String> taskNames) {
        Pattern pattern = suite.partTaskPattern();
        Set<Integer> parts = new TreeSet<>();
        for (String name : taskNames) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.find()) {
                parts.add(Integer.parseInt(matcher.group(1)));
            }
        }
        return parts;
    }

    static void configureShards(Test test, CiPartAssignment assignment, Set<Integer> requestedParts) {
        CiPartAssignment.Split split = assignment.split(test.getPath());
        if (split == null) {
            return;
        }
        Set<Integer> shards = assignment.shardsInParts(test.getPath(), requestedParts);
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
        boolean home = assignment.homePart(project.getPath()) == part;
        List<Task> selected = new ArrayList<>();
        // most projects have no bcUpgradeTest; their bcUpgradeTestPartN tasks are empty (but for assemble below)
        Task root = project.getTasks().findByName(suite.rootTask());
        if (root != null) {
            select(project, root, assignment, part, home, new HashSet<>(), selected);
        }
        if (home && extension.getIncludeAssemble().get()) {
            selected.add(project.getTasks().getByName(LifecycleBasePlugin.ASSEMBLE_TASK_NAME));
        }
        return selected;
    }

    private static void select(
        Project project,
        Task task,
        CiPartAssignment assignment,
        int part,
        boolean home,
        Set<Task> visited,
        List<Task> selected
    ) {
        if (visited.add(task) == false) {
            return;
        }
        if (task instanceof Test) {
            if (assignment.runsInPart(task.getPath(), part)) {
                selected.add(task);
            }
        } else if (task.getActions().isEmpty()) {
            for (Task dependency : task.getTaskDependencies().getDependencies(task)) {
                select(project, dependency, assignment, part, home, visited, selected);
            }
        } else {
            // work that needs test results (a coverage report, say) runs where those tests run; in the home part it
            // would pull the whole test task in again. Only this project's tasks are inspected: the root project's
            // check also reaches tasks of included builds, whose dependencies cannot be resolved from here.
            List<? extends Task> tests = task.getProject() != project
                ? List.of()
                : task.getTaskDependencies().getDependencies(task).stream().filter(dependency -> dependency instanceof Test).toList();
            if (tests.isEmpty() ? home : tests.stream().anyMatch(test -> assignment.runsInPart(test.getPath(), part))) {
                selected.add(task);
            }
        }
    }
}
