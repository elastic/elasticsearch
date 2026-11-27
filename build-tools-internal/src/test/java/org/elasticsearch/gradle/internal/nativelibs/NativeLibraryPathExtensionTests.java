/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.elasticsearch.gradle.internal.nativelibs.NativeLibraryPathExtension.Priority;
import org.gradle.api.Project;
import org.gradle.api.tasks.JavaExec;
import org.gradle.process.CommandLineArgumentProvider;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NativeLibraryPathExtensionTests {

    private Project project;
    private NativeLibraryPathExtension extension;

    @Before
    public void setUp() {
        project = ProjectBuilder.builder().build();
        project.getPlugins().apply("java");
        extension = NativeLibraryPathExtension.of(project);
    }

    /**
     * The property of the extension: the most specific configuration of a task wins, regardless of the order of
     * the calls. The priorities come from the enum, so a priority added later is covered too.
     */
    @Test
    public void testTheHighestPriorityWinsInAnyOrder() {
        Priority[] priorities = Priority.values();
        Priority highest = priorities[priorities.length - 1];

        for (List<Priority> order : permutations(Arrays.asList(priorities))) {
            JavaExec task = newTask();
            for (Priority priority : order) {
                extension.configureWithDirectories(task, directoryFor(priority), priority);
            }
            assertEquals("configured in the order " + order, Set.of(directoryFor(highest)), directoriesOf(task));
        }
    }

    @Test
    public void testTheExplicitConfigurationHasTheHighestPriority() {
        Priority[] priorities = Priority.values();
        assertEquals(Priority.EXPLICIT, priorities[priorities.length - 1]);

        JavaExec task = newTask();
        extension.configureWithDirectories(task, directoryFor(Priority.EXPLICIT));
        for (Priority priority : priorities) {
            extension.configureWithDirectories(task, directoryFor(priority), priority);
        }
        assertEquals(Set.of(directoryFor(Priority.EXPLICIT)), directoriesOf(task));
    }

    /** A task passes down one list, so a configuration replaces an earlier one with the same priority. */
    @Test
    public void testAConfigurationWithTheSamePriorityReplacesTheEarlierOne() {
        for (Priority priority : Priority.values()) {
            JavaExec task = newTask();
            extension.configureWithDirectories(task, project.file("first"), priority);
            extension.configureWithDirectories(task, project.file("second"), priority);
            assertEquals("priority " + priority, Set.of(project.file("second")), directoriesOf(task));
        }
    }

    @Test
    public void testATaskHasOneProviderWhateverTheNumberOfConfigurations() {
        JavaExec task = newTask();
        for (Priority priority : Priority.values()) {
            extension.configureWithDirectories(task, directoryFor(priority), priority);
        }
        assertEquals(1, task.getJvmArgumentProviders().stream().filter(NativeLibraryPathArgumentProvider.class::isInstance).count());
    }

    private JavaExec newTask() {
        return project.getTasks().create("task" + project.getTasks().size(), JavaExec.class);
    }

    private File directoryFor(Priority priority) {
        return project.file(priority.name());
    }

    private static Set<File> directoriesOf(JavaExec task) {
        List<NativeLibraryPathArgumentProvider> providers = new ArrayList<>();
        for (CommandLineArgumentProvider provider : task.getJvmArgumentProviders()) {
            if (provider instanceof NativeLibraryPathArgumentProvider nativeLibraryPathProvider) {
                providers.add(nativeLibraryPathProvider);
            }
        }
        assertEquals(1, providers.size());
        return providers.get(0).getLibraryDirectories().getFiles();
    }

    private static <T> List<List<T>> permutations(List<T> items) {
        if (items.isEmpty()) {
            return List.of(List.of());
        }
        List<List<T>> result = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            List<T> rest = new ArrayList<>(items);
            T head = rest.remove(i);
            for (List<T> tail : permutations(rest)) {
                List<T> permutation = new ArrayList<>();
                permutation.add(head);
                permutation.addAll(tail);
                result.add(permutation);
            }
        }
        return result;
    }
}
