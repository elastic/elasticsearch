/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ForbiddenPatternsTaskTests {

    @Test
    public void testCheckInvalidPatternsWhenNoSourceFilesExist() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        checkAndAssertTaskSuccessful(task);
    }

    @Test
    public void testCheckInvalidPatternsWhenSourceFilesExistNoViolation() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        writeSourceFile(project, "src/main/java/Foo.java", "public void bar() {}");
        checkAndAssertTaskSuccessful(task);
    }

    @Test
    public void testCheckInvalidPatternsWhenSourceFilesExistHavingTab() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        writeSourceFile(project, "src/main/java/Bar.java", "\tpublic void bar() {}");
        checkAndAssertTaskThrowsException(task);
    }

    @Test
    public void testCheckInvalidPatternsWithCustomRule() throws Exception {
        Map<String, String> rule = new HashMap<>();
        rule.put("name", "TODO comments are not allowed");
        rule.put("pattern", "\\/\\/.*(?i)TODO");

        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);
        task.rule(rule);

        writeSourceFile(project, "src/main/java/Moot.java", "GOOD LINE", "//todo", "// some stuff, toDo");
        checkAndAssertTaskThrowsException(task);
    }

    @Test
    public void testCheckInvalidPatternsWhenExcludingFiles() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);
        task.exclude("**/*.java");

        writeSourceFile(project, "src/main/java/FooBarMoot.java", "\t");
        checkAndAssertTaskSuccessful(task);
    }

    @Test
    public void testExceptionMessageContainsPerViolationDetail() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        writeSourceFile(project, "src/main/java/Bar.java", "\tpublic void bar() {}");
        try {
            task.checkInvalidPatterns();
            fail("GradleException was expected");
        } catch (GradleException e) {
            // Verify the exception message lists each violation with rule, line, and file
            assertTrue("Expected per-violation detail in message, got: " + e.getMessage(),
                e.getMessage().startsWith("Found invalid patterns:\n"));
            assertTrue("Expected tab rule reference, got: " + e.getMessage(),
                e.getMessage().contains("tab on line"));
            assertTrue("Expected file path reference, got: " + e.getMessage(),
                e.getMessage().contains("Bar.java"));
        }
    }

    @Test
    public void testMultipleViolationsCollectedInSingleException() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        // Two files with tabs - should produce multiple violations collected into one exception
        writeSourceFile(project, "src/main/java/File1.java", "\tfoo");
        writeSourceFile(project, "src/main/java/File2.java", "\tbar");
        try {
            task.checkInvalidPatterns();
            fail("GradleException was expected");
        } catch (GradleException e) {
            // Should collect all violations (at least 2) rather than failing on the first one
            String msg = e.getMessage();
            assertTrue("Expected per-violation detail, got: " + msg, msg.startsWith("Found invalid patterns:\n"));
            // Count the "- " prefixed lines
            long violationCount = msg.lines().filter(l -> l.startsWith("- ")).count();
            assertTrue("Expected at least 2 violations, found " + violationCount, violationCount >= 2);
        }
    }

    @Test
    public void testMultipleRuleViolationsInSameFile() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        // A file with both a tab and a nocommit marker
        writeSourceFile(project, "src/main/java/Bad.java", "\t// nocommit: fix this");
        try {
            task.checkInvalidPatterns();
            fail("GradleException was expected");
        } catch (GradleException e) {
            // Should detect both tab and nocommit violations
            String msg = e.getMessage();
            assertTrue("Expected per-violation detail, got: " + msg, msg.startsWith("Found invalid patterns:\n"));
            long violationCount = msg.lines().filter(l -> l.startsWith("- ")).count();
            assertTrue("Expected at least 2 violations from different rules, found " + violationCount, violationCount >= 2);
        }
    }

    // -- toKebabCase edge-case tests ------------------------------------------------

    @Test
    public void testToKebabCaseSimpleCamelCase() {
        // toKebabCase lowercases first then replaces non-alphanumeric; it does not split on camelCase boundaries
        assertEquals("nocommit", ForbiddenPatternsTask.toKebabCase("noCommit"));
    }

    @Test
    public void testToKebabCaseUpperCamelCase() {
        assertEquals("nocommit", ForbiddenPatternsTask.toKebabCase("NoCommit"));
    }

    @Test
    public void testToKebabCaseAllUpperCase() {
        assertEquals("nocommit", ForbiddenPatternsTask.toKebabCase("NOCOMMIT"));
    }

    @Test
    public void testToKebabCaseAlreadyKebab() {
        assertEquals("already-kebab", ForbiddenPatternsTask.toKebabCase("already-kebab"));
    }

    @Test
    public void testToKebabCaseWithUnderscores() {
        assertEquals("snake-case-name", ForbiddenPatternsTask.toKebabCase("snake_case_name"));
    }

    @Test
    public void testToKebabCaseWithSpaces() {
        assertEquals("with-spaces", ForbiddenPatternsTask.toKebabCase("with spaces"));
    }

    @Test
    public void testToKebabCaseLeadingAndTrailingSpecialChars() {
        assertEquals("trimmed", ForbiddenPatternsTask.toKebabCase("--trimmed--"));
    }

    @Test
    public void testToKebabCaseConsecutiveSpecialChars() {
        assertEquals("a-b", ForbiddenPatternsTask.toKebabCase("a___b"));
    }

    @Test
    public void testToKebabCaseNumericContent() {
        assertEquals("rule-42-name", ForbiddenPatternsTask.toKebabCase("rule 42 name"));
    }

    @Test
    public void testToKebabCaseSingleWord() {
        assertEquals("tabs", ForbiddenPatternsTask.toKebabCase("tabs"));
    }

    @Test
    public void testToKebabCaseEmptyString() {
        assertEquals("", ForbiddenPatternsTask.toKebabCase(""));
    }

    @Test
    public void testToKebabCaseOnlySpecialChars() {
        assertEquals("", ForbiddenPatternsTask.toKebabCase("---"));
    }

    // -- end toKebabCase tests -----------------------------------------------------

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);

        return project;
    }

    private ForbiddenPatternsTask createTask(Project project, String taskName) {
        return project.getTasks().create(taskName, ForbiddenPatternsTask.class, forbiddenPatternsTask -> {
            forbiddenPatternsTask.getSourceFolders()
                .addAll(
                    project.provider(
                        (Callable<Iterable<? extends FileTree>>) () -> GradleUtils.getJavaSourceSets(project)
                            .stream()
                            .map(s -> s.getAllSource())
                            .collect(Collectors.toList())
                    )
                );
            forbiddenPatternsTask.getRootDir().set(project.getRootDir());
        });
    }

    private ForbiddenPatternsTask createTask(Project project) {
        return createTask(project, "forbiddenPatterns");
    }

    private void writeSourceFile(Project project, String name, String... lines) throws IOException {
        File file = new File(project.getProjectDir(), name);
        file.getParentFile().mkdirs();
        file.createNewFile();

        if (lines.length != 0) Files.write(file.toPath(), Arrays.asList(lines), StandardCharsets.UTF_8);
    }

    private void checkAndAssertTaskSuccessful(ForbiddenPatternsTask task) throws IOException {
        task.checkInvalidPatterns();
        assertTaskSuccessful(task.getProject(), task.getName());
    }

    private void checkAndAssertTaskThrowsException(ForbiddenPatternsTask task) throws IOException {
        try {
            task.checkInvalidPatterns();
            fail("GradleException was expected to be thrown in this case!");
        } catch (GradleException e) {
            assertTrue("Expected message about invalid patterns, got: " + e.getMessage(),
                e.getMessage().startsWith("Found invalid patterns:\n"));
        }
    }

    private void assertTaskSuccessful(Project project, String fileName) throws IOException {
        File outputMarker = new File(project.getBuildDir(), "markers/" + fileName);
        assertTrue(outputMarker.exists());

        Optional<String> result = Files.readAllLines(outputMarker.toPath(), StandardCharsets.UTF_8).stream().findFirst();
        assertTrue(result.isPresent());
        assertEquals("done", result.get());
    }
}
