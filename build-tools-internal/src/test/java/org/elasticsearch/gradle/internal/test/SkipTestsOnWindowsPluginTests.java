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
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SkipTestsOnWindowsPluginTests {

    @Test
    public void testDisablesTestTasksOnWindows() {
        withOsName("Windows 11", () -> {
            Project project = ProjectBuilder.builder().build();
            project.getPlugins().apply("java");
            project.getPlugins().apply("elasticsearch.skip-tests-on-windows");

            org.gradle.api.tasks.testing.Test test = (org.gradle.api.tasks.testing.Test) project.getTasks().getByName("test");
            assertThat(test.getOnlyIf().isSatisfiedBy(test), is(false));
        });
    }

    @Test
    public void testAllowsTestTasksOnNonWindows() {
        withOsName("Linux", () -> {
            Project project = ProjectBuilder.builder().build();
            project.getPlugins().apply("java");
            project.getPlugins().apply("elasticsearch.skip-tests-on-windows");

            org.gradle.api.tasks.testing.Test test = (org.gradle.api.tasks.testing.Test) project.getTasks().getByName("test");
            assertThat(test.getOnlyIf().isSatisfiedBy(test), is(true));
        });
    }

    private static void withOsName(String osName, Runnable runnable) {
        String previous = System.getProperty("os.name");
        try {
            System.setProperty("os.name", osName);
            runnable.run();
        } finally {
            if (previous == null) {
                System.clearProperty("os.name");
            } else {
                System.setProperty("os.name", previous);
            }
        }
    }
}

