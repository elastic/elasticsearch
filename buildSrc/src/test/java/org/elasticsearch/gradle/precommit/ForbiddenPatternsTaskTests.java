/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ForbiddenPatternsTaskTests extends GradleUnitTestCase {

    public void testCheckInvalidPatternsWhenNoSourceFilesExist() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        checkAndAssertTaskSuccessful(task);
    }

    public void testCheckInvalidPatternsWhenSourceFilesExistNoViolation() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        writeSourceFile(project, "src/main/java/Foo.java", "public void bar() {}");
        checkAndAssertTaskSuccessful(task);
    }

    public void testCheckInvalidPatternsWhenSourceFilesExistHavingTab() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        writeSourceFile(project, "src/main/java/Bar.java", "\tpublic void bar() {}");
        checkAndAssertTaskThrowsException(task);
    }

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

    public void testCheckInvalidPatternsWhenExcludingFiles() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);
        task.exclude("**/*.java");

        writeSourceFile(project, "src/main/java/FooBarMoot.java", "\t");
        checkAndAssertTaskSuccessful(task);
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);

        return project;
    }

    private ForbiddenPatternsTask createTask(Project project) {
        return project.getTasks().create("forbiddenPatterns", ForbiddenPatternsTask.class);
    }

    private ForbiddenPatternsTask createTask(Project project, String taskName) {
        return project.getTasks().create(taskName, ForbiddenPatternsTask.class);
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
            assertTrue(e.getMessage().startsWith("Found invalid patterns"));
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
