/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ConcatFilesTaskTests {

    @Test
    public void testHeaderAdded() throws IOException {

        Project project = createProject();
        ConcatFilesTask concatFilesTask = createTask(project);

        concatFilesTask.setHeaderLine("Header");

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        concatFilesTask.setTarget(file);
        concatFilesTask.setFiles(project.fileTree("tmp/"));

        concatFilesTask.concatFiles();

        assertEquals(Arrays.asList("Header"), Files.readAllLines(concatFilesTask.getTarget().toPath(), StandardCharsets.UTF_8));

        file.delete();
    }

    @Test
    public void testConcatenationWithUnique() throws IOException {

        Project project = createProject();
        ConcatFilesTask concatFilesTask = createTask(project);

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        concatFilesTask.setTarget(file);

        File file1 = new File(project.getProjectDir(), "src/main/input/java/file1.java");
        File file2 = new File(project.getProjectDir(), "src/main/input/text/file2.txt");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file1.createNewFile();
        file2.createNewFile();
        Files.writeString(file1.toPath(), "Hello" + System.lineSeparator() + "Hello");
        Files.writeString(file2.toPath(), "Hello" + System.lineSeparator() + "नमस्ते");

        concatFilesTask.setFiles(project.fileTree(file1.getParentFile().getParentFile()));

        concatFilesTask.concatFiles();

        assertEquals(Arrays.asList("Hello", "नमस्ते"), Files.readAllLines(concatFilesTask.getTarget().toPath(), StandardCharsets.UTF_8));

    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        return project;
    }

    private ConcatFilesTask createTask(Project project) {
        return project.getTasks().create("concatFilesTask", ConcatFilesTask.class);
    }

}
