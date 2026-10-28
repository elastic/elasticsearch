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
    public void testIgnoreSuppressesViolation() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/tools/plugin-cli/bc/build.gradle",
            List.of("tasks.named(\"forbiddenApisMain\").configure {", "  classpath += project(\":server\").sourceSets.main.runtimeClasspath", "}")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("cross-project-dereference", List.of("distribution/tools/plugin-cli/bc/build.gradle")));

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForDirectBundledJdkVersionUseInDistributionScript() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/archives/build.gradle",
            List.of("if (Integer.parseInt(VersionProperties.bundledJdkMajorVersion) < 27) {", "  println 'x'", "}")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-bundled-jdk-source-of-truth"));
            assertTrue(e.getMessage().contains("distribution/archives/build.gradle:1"));
        }
    }

    @Test
    public void testFailsForClosureAssignmentInDistributionScript() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("def architecturesFor = { String platform -> ['x64'] }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-closure-assignment"));
            assertTrue(e.getMessage().contains("distribution/build.gradle:1"));
        }
    }

    @Test
    public void testAllowsClosureAssignmentOutsideDistributionScripts() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "x-pack/build.gradle", List.of("def helper = { String name -> name }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForExtClosureAssignmentInDistributionScript() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/docker/build.gradle", List.of("ext.expansions = { Architecture architecture -> [:] }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-ext-closure-api"));
            assertTrue(e.getMessage().contains("distribution/docker/build.gradle:1"));
        }
    }

    @Test
    public void testAllowsExtClosureAssignmentOutsideDistributionScripts() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "x-pack/build.gradle", List.of("ext.helper = { String name -> name }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testCountedIgnoreSuppressesExactlyTheBaselinedNumberOfViolations() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/build.gradle",
            List.of("def one = { }", "def two = { }")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::2")));

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testCountedIgnoreFailsWhenAdditionalViolationIsAddedToBaselinedFile() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/build.gradle",
            List.of("def one = { }", "def two = { }", "def three = { }")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::2")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-closure-assignment"));
            assertTrue(e.getMessage().contains("distribution/build.gradle:3"));
        }
    }

    @Test
    public void testCountedIgnoreFailsWhenBaselineCountBecomesStale() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("def one = { }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::2")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("stale-ignore-entry"));
            assertTrue(e.getMessage().contains("distribution-closure-assignment -> distribution/build.gradle::2"));
        }
    }

    @Test
    public void testFailsForStaleIgnoreEntry() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("cross-project-dereference", List.of("subproject/build.gradle")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("stale-ignore-entry"));
            assertTrue(e.getMessage().contains("cross-project-dereference -> subproject/build.gradle"));
        }
    }

    @Test
    public void testInvalidIgnoreFormatFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("", List.of("subproject/build.gradle")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Ignore rule ids must not be blank"));
        }
    }

    @Test
    public void testBlankIgnorePathFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("cross-project-dereference", List.of("  ")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Ignore path for rule [cross-project-dereference] must not be blank"));
        }
    }

    @Test
    public void testInvalidIgnoreCountFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("def one = { }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::abc")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Ignore count for rule [distribution-closure-assignment] must be an integer"));
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
