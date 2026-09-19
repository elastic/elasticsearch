/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checks;

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValidateBuildGradleScriptsTaskTests {

    @Test
    public void testSucceedsForDependencyProjectNotation() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForCrossProjectDereference() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/tools/plugin-cli/bc/build.gradle",
            List.of("tasks.named(\"forbiddenApisMain\").configure {", "  classpath += project(\":server\").sourceSets.main.runtimeClasspath", "}")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("cross-project-dereference"));
            assertTrue(e.getMessage().contains("distribution/tools/plugin-cli/bc/build.gradle:2"));
            assertTrue(e.getMessage().contains("project(\":server\").sourceSets.main.runtimeClasspath"));
        }
    }

    @Test
    public void testFailsForCrossProjectConfigurationClosure() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("project(\":server\") {", "  version = version", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("cross-project-configuration"));
            assertTrue(e.getMessage().contains("distribution/build.gradle:1"));
        }
    }

    @Test
    public void testBaselineSuppressesViolation() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/tools/plugin-cli/bc/build.gradle",
            List.of("tasks.named(\"forbiddenApisMain\").configure {", "  classpath += project(\":server\").sourceSets.main.runtimeClasspath", "}")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getBaseline().set(java.util.Map.of("cross-project-dereference", List.of("distribution/tools/plugin-cli/bc/build.gradle")));

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForStaleBaselineEntry() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getBaseline().set(java.util.Map.of("cross-project-dereference", List.of("subproject/build.gradle")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("stale-baseline-entry"));
            assertTrue(e.getMessage().contains("cross-project-dereference -> subproject/build.gradle"));
        }
    }

    @Test
    public void testInvalidBaselineFormatFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getBaseline().set(java.util.Map.of("", List.of("subproject/build.gradle")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Baseline rule ids must not be blank"));
        }
    }

    @Test
    public void testBlankBaselinePathFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getBaseline().set(java.util.Map.of("cross-project-dereference", List.of("  ")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Baseline path for rule [cross-project-dereference] must not be blank"));
        }
    }

    private Project createProject() throws IOException {
        File projectDir = Files.createTempDirectory("validate-build-gradle-scripts-task").toFile();
        return ProjectBuilder.builder().withProjectDir(projectDir).build();
    }

    private ValidateBuildGradleScriptsTask createTask(Project project, File... scriptFiles) {
        ValidateBuildGradleScriptsTask task = project.getTasks().create("validateBuildGradleScripts", ValidateBuildGradleScriptsTask.class);
        task.getScriptFiles().from((Object[]) scriptFiles);
        return task;
    }

    private static File writeFile(Project project, String relativePath, List<String> lines) throws IOException {
        File file = new File(project.getProjectDir(), relativePath);
        file.getParentFile().mkdirs();
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        return file;
    }

    private static void assertOutputMarkerWritten(ValidateBuildGradleScriptsTask task) throws IOException {
        File marker = task.getOutputMarker();
        assertTrue(marker.exists());
        assertEquals("done", Files.readString(marker.toPath(), StandardCharsets.UTF_8));
    }
}
