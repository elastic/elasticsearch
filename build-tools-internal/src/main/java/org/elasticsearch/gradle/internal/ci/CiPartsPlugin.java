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
 * checked-in {@link CiPartAssignment}.
 *
 * <p>{@code checkPartN} depends on the subset of {@code check}'s dependencies that belongs to part N: the test
 * tasks assigned to N, and, when N is the project's home part, everything else {@code check} runs (precommit
 * and other non-test verification). Lifecycle tasks are flattened so that test tasks behind them (for example
 * an aggregate of several test tasks) are still placed individually. Because the selection is derived from
 * {@code check} itself, running every part runs exactly what {@code check} runs.
 *
 * <p>A test task the assignment splits into shards is the same Gradle task in every part that runs one of its
 * shards; when the build is asked for {@code checkPartN} tasks, the plugin narrows each split task to the shards
 * of the requested parts, either by excluding the test classes of other shards or, for YAML suites, through the
 * {@code tests.rest.shard} system property read by {@code ESClientYamlSuiteTestCase}. Every other invocation
 * (including {@code check} and reproducing a failure) runs the whole task.
 */
public abstract class CiPartsPlugin implements Plugin<Project> {

    public static final String ASSIGNMENT_FILE = ".ci/test-parts/assignment.json";
    public static final String CHECK_PART_TASK_PREFIX = "checkPart";
    static final String REST_TESTS_SHARD_PROPERTY = "tests.rest.shard";
    private static final Pattern CHECK_PART_TASK = Pattern.compile("(?:^|:)" + CHECK_PART_TASK_PREFIX + "(\\d+)$");

    @Inject
    protected abstract ProviderFactory getProviders();

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);
        Provider<CiPartsBuildService> service = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(
                "ciParts",
                CiPartsBuildService.class,
                spec -> spec.getParameters()
                    .getAssignmentJson()
                    .set(getProviders().fileContents(project.getLayout().getSettingsDirectory().file(ASSIGNMENT_FILE)).getAsText())
            );
        CiPartsExtension extension = project.getExtensions().create("ciParts", CiPartsExtension.class);
        extension.getIncludeAssemble().convention(false);

        CiPartAssignment assignment = service.get().assignment();
        for (int part = 1; part <= assignment.parts(); part++) {
            int thisPart = part;
            project.getTasks().register(CHECK_PART_TASK_PREFIX + part, t -> {
                t.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
                t.setDescription("Runs the share of 'check' assigned to CI part " + thisPart + " of " + assignment.parts() + ".");
                t.dependsOn((Callable<List<Task>>) () -> tasksForPart(project, assignment, extension, thisPart));
            });
        }

        Set<Integer> requestedParts = requestedParts(project.getGradle().getStartParameter().getTaskNames());
        if (requestedParts.isEmpty() == false) {
            project.getTasks().withType(Test.class).configureEach(test -> configureShards(test, assignment, requestedParts));
        }
    }

    /** The parts named by {@code checkPartN} task requests, e.g. {@code checkPart3} or {@code :server:checkPart3}. */
    static Set<Integer> requestedParts(List<String> taskNames) {
        Set<Integer> parts = new TreeSet<>();
        for (String name : taskNames) {
            Matcher matcher = CHECK_PART_TASK.matcher(name);
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

    static List<Task> tasksForPart(Project project, CiPartAssignment assignment, CiPartsExtension extension, int part) {
        boolean home = assignment.homePart(project.getPath()) == part;
        Task check = project.getTasks().getByName(LifecycleBasePlugin.CHECK_TASK_NAME);
        List<Task> selected = new ArrayList<>();
        Set<Task> visited = new HashSet<>();
        for (Task dependency : check.getTaskDependencies().getDependencies(check)) {
            select(dependency, assignment, part, home, visited, selected);
        }
        if (home && extension.getIncludeAssemble().get()) {
            selected.add(project.getTasks().getByName(LifecycleBasePlugin.ASSEMBLE_TASK_NAME));
        }
        return selected;
    }

    private static void select(Task task, CiPartAssignment assignment, int part, boolean home, Set<Task> visited, List<Task> selected) {
        if (visited.add(task) == false) {
            return;
        }
        if (task instanceof Test) {
            if (assignment.runsInPart(task.getPath(), part)) {
                selected.add(task);
            }
        } else if (task.getActions().isEmpty()) {
            for (Task dependency : task.getTaskDependencies().getDependencies(task)) {
                select(dependency, assignment, part, home, visited, selected);
            }
        } else if (home) {
            selected.add(task);
        }
    }
}
