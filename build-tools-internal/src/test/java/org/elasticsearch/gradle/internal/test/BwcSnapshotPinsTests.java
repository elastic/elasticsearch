/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.process.CommandLineArgumentProvider;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

/**
 * A bwc test JVM gets the DRA builds the build resolved for its bwc versions, so that its reproduce line runs against
 * the same snapshots without looking up the known-good store or the latest build again.
 */
public class BwcSnapshotPinsTests {

    @After
    public void clearPins() {
        System.clearProperty("tests.bwc.dra.hash.9.4");
    }

    @org.junit.Test
    public void testPinsEachResolvedBuildOfTheTasksVersions() {
        Project project = ProjectBuilder.builder().build();
        Provider<BwcSnapshotBuildsService> builds = BwcSnapshotBuildsService.register(project.getGradle());
        builds.get().register("9.4.10", project.provider(() -> "9.4.10-3710ab3e"));
        builds.get().register("8.19.8", project.provider(() -> "8.19.8-aaaa1111"));
        builds.get().register("9.5.2", project.provider(() -> "9.5.2-bbbb2222"));
        Test task = bwcTestTask(project);

        BwcSnapshotPins.pin(task, "9.4.10", "9.4");
        BwcSnapshotPins.pin(task, "8.19.8", "8.19");
        BwcSnapshotPins.pin(task, "9.4.10", "9.4");

        assertThat(
            jvmArguments(task),
            contains("-Dtests.bwc.mode=dra", "-Dtests.bwc.dra.hash.8.19=aaaa1111", "-Dtests.bwc.dra.hash.9.4=3710ab3e")
        );
    }

    @org.junit.Test
    public void testLeavesOutVersionsBuiltFromSource() {
        Project project = ProjectBuilder.builder().build();
        Provider<BwcSnapshotBuildsService> builds = BwcSnapshotBuildsService.register(project.getGradle());
        builds.get().register("9.4.10", project.provider(() -> "9.4.10-3710ab3e"));
        builds.get().register("8.19.8", project.provider(() -> ""));
        Test task = bwcTestTask(project);

        BwcSnapshotPins.pin(task, "9.4.10", "9.4");
        BwcSnapshotPins.pin(task, "8.19.8", "8.19");

        assertThat(jvmArguments(task), contains("-Dtests.bwc.mode=dra", "-Dtests.bwc.dra.hash.9.4=3710ab3e"));
    }

    @org.junit.Test
    public void testPinsNothingWhenEveryVersionIsBuiltFromSource() {
        Project project = ProjectBuilder.builder().build();
        BwcSnapshotBuildsService.register(project.getGradle()).get().register("9.4.10", project.provider(() -> ""));
        Test task = bwcTestTask(project);

        BwcSnapshotPins.pin(task, "9.4.10", "9.4");
        // a version no bwc project resolves, such as a released one
        BwcSnapshotPins.pin(task, "9.3.0", "9.3");

        assertThat(jvmArguments(task), empty());
    }

    @org.junit.Test
    public void testReadsBuildsRegisteredAfterTheTaskIsConfigured() {
        Project project = ProjectBuilder.builder().build();
        Test task = bwcTestTask(project);

        BwcSnapshotPins.pin(task, "9.4.10", "9.4");
        BwcSnapshotBuildsService.register(project.getGradle()).get().register("9.4.10", project.provider(() -> "9.4.10-3710ab3e"));

        assertThat(jvmArguments(task), contains("-Dtests.bwc.mode=dra", "-Dtests.bwc.dra.hash.9.4=3710ab3e"));
    }

    @org.junit.Test
    public void testDoesNotForwardTheBuildsOwnPins() {
        System.setProperty("tests.bwc.dra.hash.9.4", "cccc3333");
        Project project = ProjectBuilder.builder().build();
        BwcSnapshotBuildsService.register(project.getGradle()).get().register("9.4.10", project.provider(() -> "9.4.10-3710ab3e"));
        Test task = bwcTestTask(project);

        BwcSnapshotPins.pin(task, "9.4.10", "9.4");

        assertThat(jvmArguments(task), contains("-Dtests.bwc.mode=dra", "-Dtests.bwc.dra.hash.9.4=3710ab3e"));
    }

    private static Test bwcTestTask(Project project) {
        return project.getTasks().register("v9.4.10#bwcTest", Test.class).get();
    }

    private static List<String> jvmArguments(Test task) {
        List<String> arguments = new ArrayList<>();
        for (CommandLineArgumentProvider provider : task.getJvmArgumentProviders()) {
            provider.asArguments().forEach(arguments::add);
        }
        return arguments;
    }
}
