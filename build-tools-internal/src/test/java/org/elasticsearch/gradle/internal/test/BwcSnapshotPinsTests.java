/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.gradle.api.Project;
import org.gradle.api.tasks.testing.Test;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

/**
 * A bwc test JVM needs the DRA snapshot pins the build ran with, so that its reproduce line runs against the same
 * snapshots. The pins are system properties of the Gradle JVM here, as they are when CI passes them with -D.
 */
public class BwcSnapshotPinsTests {

    private static final List<String> PROPERTIES = List.of("tests.bwc.mode", "tests.bwc.dra.hash.9.4", "tests.bwc.dra.hash.8.19");

    @After
    public void clearPins() {
        PROPERTIES.forEach(System::clearProperty);
    }

    @org.junit.Test
    public void testForwardsModeAndHashesToTheTestJvm() {
        System.setProperty("tests.bwc.mode", "auto");
        System.setProperty("tests.bwc.dra.hash.9.4", "bbbb2222");
        System.setProperty("tests.bwc.dra.hash.8.19", "aaaa1111");

        assertThat(
            forwardedArguments(),
            contains("-Dtests.bwc.mode=auto", "-Dtests.bwc.dra.hash.8.19=aaaa1111", "-Dtests.bwc.dra.hash.9.4=bbbb2222")
        );
    }

    @org.junit.Test
    public void testForwardsNothingWithoutPins() {
        assertThat(forwardedArguments(), empty());
    }

    private static List<String> forwardedArguments() {
        Project project = ProjectBuilder.builder().build();
        Test task = project.getTasks().register("v9.4.10#bwcTest", Test.class).get();
        SystemPropertyCommandLineArgumentProvider nonInputProperties = new SystemPropertyCommandLineArgumentProvider();
        task.getExtensions().add("nonInputProperties", nonInputProperties);

        BwcSnapshotPins.forward(task, project.getProviders());

        List<String> arguments = new ArrayList<>();
        nonInputProperties.asArguments().forEach(arguments::add);
        return arguments;
    }
}
